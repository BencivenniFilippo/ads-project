import asyncio
import os
import logging
import time

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

SERVER_CONNECTIONS = [0] * len(SERVERS)
SERVER_WEIGHTS = os.getenv("WEIGHTS", "")
if SERVER_WEIGHTS != "":
    SERVER_WEIGHTS = list(map(int, SERVER_WEIGHTS.split(",")))

SERVER_TIMES = [None] * len(SERVERS)


def update_server_ewma(server, elapsed, alpha=0.2):
    index = SERVERS.index(server)
    if SERVER_TIMES[index] is None:
        SERVER_TIMES[index] = elapsed
    else:
        updated_time = (SERVER_TIMES[index] * (1 - alpha) + elapsed * alpha)

        SERVER_TIMES[index] = updated_time


# Uses the least connections or weighted least connections methods
def choose_server(algorithm: str = "least_connections") -> tuple[str, int, int]:

    if algorithm == "least_response_time":

        for server, elapsed in zip(SERVERS, SERVER_TIMES):
            if elapsed is None:
                return server[0], int(server[1]), SERVERS.index(server)

        i = SERVER_TIMES.index(min(SERVER_TIMES))

        server = SERVERS[i]

        return server[0], int(server[1]), SERVERS.index(server)


    elif algorithm == "weighted_least_connections":
        connections = [c/w for c,w in zip(SERVER_CONNECTIONS, SERVER_WEIGHTS)]

    elif algorithm == "least_connections":
        connections = SERVER_CONNECTIONS

    else:
        raise ValueError(f"The selected algorithm {algorithm} isnt available.")
    
    index = connections.index(min(connections))
    server = SERVERS[index]
    return server[0], int(server[1]), index


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while not reader.at_eof():
            data = await reader.read(4096)
            if not data:
                break
            writer.write(data)  # Queue data in the writer's outgoing buffer
            await writer.drain()  # Wait if the buffer is full to write the read data
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(e)
    finally:
        writer.close()


async def client_handler(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter, algorithm: str = "least_connections"):
    server_index = None
    elapsed = None
    try:
        server_host, server_port, server_index = choose_server(algorithm)  # Add True to use weighted method
        SERVER_CONNECTIONS[server_index] += 1

        server_reader, server_writer = await asyncio.open_connection(server_host, server_port)  # TCP connection LB -> server

        start = time.perf_counter()

        # Create pipes to forward byte streams over the existing TCP connections
        client_to_server = asyncio.create_task(pipe(client_reader, server_writer)) # client -> LB -> server
        server_to_client = asyncio.create_task(pipe(server_reader, client_writer)) # server -> LB -> client

        await asyncio.gather(client_to_server, server_to_client, return_exceptions=True)

        elapsed = time.perf_counter() - start

    except Exception as e:
        logger.error(e)
        client_writer.close()
    
    finally:
        if algorithm == "least_response_time": # TODO: can this if be optimized instead of running each request?
            if elapsed is not None:
                update_server_ewma(server_host, elapsed) # Update avg server transaction time
            else: # TODO
                pass
        
        if server_index is not None:
            SERVER_CONNECTIONS[server_index] -= 1


async def main():
    port = os.getenv("BIND_PORT", "7777")
    algorithm = os.getenv("LB_ALGORITHM", "least_connections")

    lb = await asyncio.start_server(client_handler, port=port, algorithm=algorithm) # Create TCP LB server

    async with lb:
        await lb.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
