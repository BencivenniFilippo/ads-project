import logging
import os

import redis
import rpyc
from rpyc.utils.server import ThreadedServer

SERVER_NAME = os.getenv("SERVER_NAME", "server")
logging.basicConfig(
    level=logging.WARNING,
    format=f'%(asctime)s - [{SERVER_NAME}] - %(levelname)s - %(message)s',
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(SERVER_NAME)
logger.setLevel(logging.INFO)
request_count = 0
SUMMARY_INTERVAL = 100
cache_hits = 0
cache_misses = 0

r = redis.Redis(host=os.getenv("CACHE_HOST", "cache"), port=int(os.getenv("CACHE_PORT", "6379")))


def word_count(filename: str, target_word: str) -> int:
    with open(filename, "r") as f:
        text = f.readlines()

        count = 0

        for line in text:
            # Replace any character that isn't a letter or apostrophe with a space
            clean_line = "".join(char if char.isalpha() or char == "'" else " " for char in line)
            
            for word in clean_line.split():
                # Remove any leading/trailing apostrophes and convert to lowercase
                word = word.strip("'").lower()
                
                if word and word == target_word:
                    count += 1
    return(count)

class WordCountService(rpyc.Service):
    def exposed_word_count(self, file: str, word: str) -> int:
        global request_count, cache_hits, cache_misses
        request_count += 1
        word = word.lower()
        raw_value = r.get(file + "/" + word)

        if raw_value is not None:
            cache_hits += 1
            logger.debug("Retrieved from Redis: " + str(raw_value))
            count = int(raw_value)
        else:
            cache_misses += 1
            logger.debug("No value found in Redis")

            filepath = os.path.join(os.path.dirname(__file__), "Texts", file)
            count = word_count(filepath, word)
            r.set(file + "/" + word, count)

        if request_count == 1 or request_count % SUMMARY_INTERVAL == 0:
            logger.info(
                "handled_requests=%d cache_hits=%d cache_misses=%d "
                "latest=%s/%s result=%d",
                request_count,
                cache_hits,
                cache_misses,
                file,
                word,
                count,
            )
        return count


    def exposed_txt_file_list(self) -> list[str]:
        return [file for file in os.listdir(os.path.join(os.path.dirname(__file__), "Texts")) if file.endswith(".txt")]

    def exposed_read_file(self, file: str) -> str:
        filepath = os.path.join(os.path.dirname(__file__), "Texts", file)
        with open(filepath, "r") as f:
            return f.read()


if __name__ == "__main__":
    port = int(os.getenv("RPC_PORT", "7777"))
    logger.info(
        "starting server on port %d with cache host %s",
        port,
        os.getenv("CACHE_HOST", "cache"),
    )
    server = ThreadedServer(WordCountService, port=port)
    server.start()