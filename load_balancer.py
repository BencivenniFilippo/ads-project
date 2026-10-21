import asyncio
import concurrent.futures
from functools import partial
import os
import logging
import time

import rpyc

lb_name = os.getenv("LB_NAME", "load_balancer")
logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - [{lb_name}] - %(levelname)s - %(message)s',
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(lb_name)

SERVER_ENV = os.getenv("SERVERS", "")
if SERVER_ENV == "":
    raise ValueError("SERVERS environment variable is empty")

try:
    server_parts = [s.strip().split(":") for s in SERVER_ENV.split(",")]
    if any(len(parts) != 2 or not parts[0] or not parts[1] for parts in server_parts):
        raise ValueError
    SERVERS = [tuple(parts) for parts in server_parts]
except ValueError:
    logger.error("Invalid server format in SERVERS environment variable")
    raise SystemExit(1)

SERVER_UP = [False] * len(SERVERS)
SERVER_CONNECTIONS = [0] * len(SERVERS)
SERVER_WEIGHTS = os.getenv("WEIGHTS", "")
if SERVER_WEIGHTS != "":
    try:
        SERVER_WEIGHTS = [int(weight) for weight in SERVER_WEIGHTS.split(",")]
        if len(SERVER_WEIGHTS) != len(SERVERS) or any(weight <= 0 for weight in SERVER_WEIGHTS):
            raise ValueError
    except ValueError:
        logger.error("WEIGHTS must contain one positive integer per server")
        raise SystemExit(1)

SERVER_TIMES = [None] * len(SERVERS)
EWMA_ALPHA = 0.2
SUMMARY_INTERVAL = 100
SERVER_HANDLED = [0] * len(SERVERS)
TOTAL_TRANSACTIONS = 0


def update_server_ewma(server_index: int, elapsed: float) -> None:
    if SERVER_TIMES[server_index] is None:
        SERVER_TIMES[server_index] = elapsed
    else:
        SERVER_TIMES[server_index] = (
            SERVER_TIMES[server_index] * (1 - EWMA_ALPHA)
            + elapsed * EWMA_ALPHA
        )


# Picks a backend among the servers currently marked UP.
# Raises ValueError when no server is available.
def choose_server(algorithm: str = "least_connections") -> tuple[str, int, int]:
    up = [i for i in range(len(SERVERS)) if SERVER_UP[i]]
    if not up:
        raise ValueError("no server available")

    if algorithm == "least_response_time":
        # Servers without any measurement yet are tried first
        untested = [i for i in up if SERVER_TIMES[i] is None]
        if untested:
            index = untested[0]
        else:
            index = min(up, key=lambda i: SERVER_TIMES[i])
    elif algorithm == "weighted_least_connections":
        index = min(up, key=lambda i: SERVER_CONNECTIONS[i] / SERVER_WEIGHTS[i])
    else:
        index = min(up, key=lambda i: SERVER_CONNECTIONS[i])

    server = SERVERS[index]
    return server[0], int(server[1]), index


async def buffering_pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, buffer: bytearray):
    try:
        # If this is a retry attempt, replay all previously buffered data first
        if buffer:
            writer.write(buffer)
            await writer.drain()

        # Continue reading new data from the client and adding it to the buffer
        while not reader.at_eof():
            data = await reader.read(4096)
            if not data:
                break
            buffer.extend(data)
            writer.write(data)
            await writer.drain()

        if writer.can_write_eof():
            writer.write_eof()
    except asyncio.CancelledError:
        pass


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while not reader.at_eof():
            data = await reader.read(4096)
            if not data:
                break
            try:
                writer.write(data)
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError):
                # Client closed its socket; do not blame the backend server
                return False
        return True
    except asyncio.CancelledError:
        return False


NO_SERVER_WAIT_TIMEOUT = float(os.getenv("NO_SERVER_WAIT_TIMEOUT", "5"))


async def wait_for_any_server(timeout: float) -> bool:
    deadline = time.perf_counter() + timeout
    while not any(SERVER_UP):
        if time.perf_counter() >= deadline:
            return False
        await asyncio.sleep(0.1)
    return True


async def client_handler(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter, algorithm: str = "least_connections"):
    global TOTAL_TRANSACTIONS
    client_buffer = bytearray()

    while True:
        # At startup the monitor may not have marked any backend as UP yet.
        # Wait briefly instead of dropping the client connection immediately.
        if not await wait_for_any_server(NO_SERVER_WAIT_TIMEOUT):
            logger.error("No available servers left to handle the request.")
            break

        server_index = None
        server_writer = None
        client_to_server = None
        server_to_client = None
        elapsed = None
        transaction_succeeded = False

        try:
            server_host, server_port, server_index = choose_server(algorithm)
            SERVER_CONNECTIONS[server_index] += 1

            start = time.perf_counter()
            # Short timeout: a dead container can leave the TCP connect hanging for ~30 s
            server_reader, server_writer = await asyncio.wait_for(
                asyncio.open_connection(server_host, server_port), timeout=1.0
            )

            client_to_server = asyncio.create_task(
                buffering_pipe(client_reader, server_writer, client_buffer)
            )
            server_to_client = asyncio.create_task(
                pipe(server_reader, client_writer)
            )

            done, pending = await asyncio.wait(
                [client_to_server, server_to_client],
                return_when=asyncio.FIRST_COMPLETED
            )

            # 1. If the finished task raised an exception (e.g. server reset connection), re-raise it
            for task in done:
                exc = task.exception()
                if exc:
                    for p in pending:
                        p.cancel()
                    raise exc

            # 2. If server_to_client finished first, check if the client is also closing normally
            if server_to_client in done and client_to_server in pending:
                try:
                    # On normal conn.close(), client_to_server hits EOF within ~1ms
                    await asyncio.wait_for(client_to_server, timeout=0.2)
                except asyncio.TimeoutError:
                    # Client is still waiting for a response -> server died mid-request
                    client_to_server.cancel()
                    raise ConnectionResetError("Server closed connection unexpectedly mid-stream")

            # 3. If client_to_server finished first (client closed), let server_to_client flush
            if client_to_server in done and server_to_client in pending:
                try:
                    await asyncio.wait_for(server_to_client, timeout=1.0)
                except asyncio.TimeoutError:
                    server_to_client.cancel()

            # Request completed cleanly
            elapsed = time.perf_counter() - start
            transaction_succeeded = True
            SERVER_HANDLED[server_index] += 1
            TOTAL_TRANSACTIONS += 1
            if TOTAL_TRANSACTIONS % SUMMARY_INTERVAL == 0:
                routing = " ".join(
                    f"{host}={count}"
                    for (host, _), count in zip(SERVERS, SERVER_HANDLED)
                )
                logger.info(
                    "routing summary | total=%d | %s",
                    TOTAL_TRANSACTIONS,
                    routing,
                )

            # If both pipes close cleanly without exceptions, the request is fully complete
            break

        except ValueError:
            logger.error("No available servers left to handle the request.")
            break

        except Exception as e:
            logger.warning(f"Connection failed mid-stream: {e}. Re-routing and replaying buffer...")
            if server_index is not None:
                SERVER_UP[server_index] = False

        finally:
            if (
                    algorithm == "least_response_time"
                    and server_index is not None
                    and elapsed is not None
                    and transaction_succeeded
            ):
                update_server_ewma(server_index, elapsed)
            if server_index is not None:
                SERVER_CONNECTIONS[server_index] -= 1
            if server_writer is not None:
                server_writer.close()

    client_writer.close()


def ping_server(conn) -> bool:
    """Blocking ping on an existing connection (run in a thread)."""
    try:
        conn.ping(timeout=2)
        return True
    except Exception:
        return False


def connect_server(host: str, port: int):
    """Blocking connect (run in a thread). Returns a connection or None.

    Resolving the name of a dead container can take several seconds (Docker DNS),
    so this must never run in the same place as the other servers' probes.
    """
    try:
        return rpyc.connect(host, port)
    except Exception:
        return None


async def monitor_one_server(i: int):
    """Health loop of a single backend. Each server has its own loop, so a slow
    probe of a dead server never delays the status updates of the others."""
    host, port = SERVERS[i][0], int(SERVERS[i][1])
    conn = None
    while True:
        try:
            if conn is not None and not await asyncio.to_thread(ping_server, conn):
                # Mark it down immediately, before any (slow) reconnection attempt
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
                if SERVER_UP[i]:
                    logger.warning("server %s is DOWN", host)
                SERVER_UP[i] = False
            if conn is None:
                conn = await asyncio.to_thread(connect_server, host, port)
                if conn is not None:
                    if not SERVER_UP[i]:
                        logger.info("server %s is UP", host)
                    SERVER_UP[i] = True
                elif SERVER_UP[i]:
                    logger.warning("server %s is DOWN", host)
                    SERVER_UP[i] = False
            elif not SERVER_UP[i]:
                # A request failed earlier but the server still answers pings
                logger.info("server %s is UP", host)
                SERVER_UP[i] = True
        except Exception as e:
            logger.error("monitor error for %s: %s", host, e)
        await asyncio.sleep(1)


async def server_monitoring():
    await asyncio.gather(*[monitor_one_server(i) for i in range(len(SERVERS))])


async def main():
    # Name resolution (getaddrinfo) and health probes run in threads. A dead
    # container can block a lookup for several seconds, so the default pool
    # (about 16 threads) would be exhausted and healthy servers would time out.
    asyncio.get_running_loop().set_default_executor(
        concurrent.futures.ThreadPoolExecutor(max_workers=512)
    )
    port = os.getenv("BIND_PORT", "7777")
    sm = asyncio.create_task(server_monitoring())
    algorithm = os.getenv("LB_ALGORITHM", "least_connections")
    backends = ",".join(f"{host}:{server_port}" for host, server_port in SERVERS)
    logger.info(
        "starting load balancer | port=%s | algorithm=%s | backends=%s",
        port,
        algorithm,
        backends,
    )

    handler = partial(client_handler, algorithm=algorithm)
    lb = await asyncio.start_server(handler, port=port) # Create TCP LB server

    async with lb:
        await lb.serve_forever()

    sm.cancel()


if __name__ == "__main__":
    asyncio.run(main())
