import os
import subprocess

import redis
import rpyc
from rpyc.utils.server import ThreadedServer


r = redis.Redis(host='cache', port=6379)


class WordCountService(rpyc.Service):
    def exposed_word_count(self, file: str, word: str) -> int:
        raw_value = r.get(file + "/" + word)
        if raw_value is not None:
            return int(raw_value)
        filepath = os.path.join(os.path.dirname(__file__), "Texts", file)
        out = subprocess.check_output(f'grep -o -i "\\b{word}\\b" {filepath} | wc -l', shell=True)
        try:
            count = int(out)
        except ValueError:
            return -1
        r.set(file + "/" + word, count)
        return count


if __name__ == "__main__":
    server = ThreadedServer(WordCountService, port=int(os.getenv('RPC_PORT', 7777)))
    server.start()