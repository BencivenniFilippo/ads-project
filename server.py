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

r = redis.Redis(host=os.getenv("CACHE_HOST", "cache"), port=os.getenv("CACHE_PORT", "6379"))


def word_count(filename: str, target_word: str) -> int:
    with open(filename, "r") as f:
        text = f.readlines()

        count = 0

        for line in text:
            clean_line = line.strip()
            if clean_line:
                for word in clean_line.split():
                    if word.lower() == target_word:
                        count += 1

    return(count)

class WordCountService(rpyc.Service):
    def exposed_word_count(self, file: str, word: str) -> int:
        word = word.lower()
        raw_value = r.get(file + "/" + word)

        if raw_value is not None:
            logger.debug("Retreived from Redis: " + str(raw_value))
            return int(raw_value)
        
        logger.debug("No value found in Redis")

        filepath = os.path.join(os.path.dirname(__file__), "Texts", file)

        out = word_count(filepath, word)
        #out = subprocess.check_output(f'grep -o -i "\\b{word}\\b" {filepath} | wc -l', shell=True)

        try:
            count = int(out)
        except ValueError:
            return -1
        r.set(file + "/" + word, count)
        return count


    def exposed_txt_file_list(self) -> list[str]:
        return [file for file in os.listdir(os.path.join(os.path.dirname(__file__), "Texts")) if file.endswith(".txt")]

    def exposed_read_file(self, file: str) -> str:
        filepath = os.path.join(os.path.dirname(__file__), "Texts", file)
        with open(filepath, "r") as f:
            return f.read()


if __name__ == "__main__":
    server = ThreadedServer(WordCountService, port=int(os.getenv('RPC_PORT', "7777")))
    server.start()