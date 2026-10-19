import asyncio
import os
import logging

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


# Uses the least connections or weighted least connections methods
def choose_server(weighted = False) -> tuple[str, int, int]:
    if weighted:
        connections = [c/w for c,w in zip(SERVER_CONNECTIONS, SERVER_WEIGHTS)]
    else:
        connections = SERVER_CONNECTIONS
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


async def client_handler(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
    server_index = None
    try:
        server_host, server_port, server_index = choose_server()  # Add True to use weighted method
        SERVER_CONNECTIONS[server_index] += 1
        server_reader, server_writer = await asyncio.open_connection(server_host, server_port)  # TCP connection LB -> server

        # Create pipes to forward byte streams over the existing TCP connections
        client_to_server = asyncio.create_task(pipe(client_reader, server_writer)) # client -> LB -> server
        server_to_client = asyncio.create_task(pipe(server_reader, client_writer)) # server -> LB -> client

        await asyncio.gather(client_to_server, server_to_client, return_exceptions=True)

    except Exception as e:
        logger.error(e)
        client_writer.close()
    
    finally:
        if server_index is not None:
            SERVER_CONNECTIONS[server_index] -= 1


async def main():
    port = os.getenv("BIND_PORT", "7777")
    lb = await asyncio.start_server(client_handler, port=port) # Create TCP LB server

    async with lb:
        await lb.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
