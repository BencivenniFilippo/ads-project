import os
import subprocess

import redis
import rpyc
from rpyc.utils.server import ThreadedServer
import logging

server_name = os.getenv("SERVER_NAME", "server")
logging.basicConfig(
    level=logging.WARNING,
    format=f'%(asctime)s - [{server_name}] - %(levelname)s - %(message)s',
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(server_name)

r = redis.Redis(host='cache', port=6379)


class WordCountService(rpyc.Service):
    def exposed_word_count(self, file: str, word: str) -> int:
        word = word.lower()
        raw_value = r.get(file + "/" + word)

        if raw_value is not None:
            logger.debug("Retreived from Redis: " + str(raw_value))
            return int(raw_value)
        
        logger.debug("No value found in Redis")

        filepath = os.path.join(os.path.dirname(__file__), "Texts", file)
        out = subprocess.check_output(f'grep -o -i "\\b{word}\\b" {filepath} | wc -l', shell=True)
        try:
            count = int(out)
        except ValueError:
            return -1
        r.set(file + "/" + word, count)
        return count


if __name__ == "__main__":
    server = ThreadedServer(WordCountService, port=int(os.getenv('RPC_PORT', "7777")))
    server.start()