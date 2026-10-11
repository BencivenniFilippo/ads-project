import asyncio
import os
import logging

SERVER_ENV = os.getenv("SERVERS", "")
SERVERS = [tuple(s.split(":")) for s in SERVER_ENV.split(",")]
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
            writer.write(data)
            await writer.drain()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.error(e)
    finally:
        writer.close()


async def client_handler(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
    server_host, server_port, server_index = choose_server() # Add True to use weighted method
    server_index += 1
    try:
        server_reader, server_writer = await asyncio.open_connection(server_host, server_port)
    except Exception as e:
        logging.error(e)
        client_writer.close()
        return

    client_to_server = asyncio.create_task(pipe(client_reader, server_writer))
    server_to_client = asyncio.create_task(pipe(server_reader, client_writer))

    await asyncio.gather(client_to_server, server_to_client, return_exceptions=True)
    server_index -= 1


async def main():
    port = os.getenv("BIND_PORT", "7777")
    lb = await asyncio.start_server(client_handler, port=port)

    async with lb:
        await lb.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
