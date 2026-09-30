import os
import subprocess

import rpyc
from rpyc.utils.server import ThreadedServer


class WordCountService(rpyc.Service):
    def exposed_word_count(self, file: str, word: str) -> int:
        filepath = os.path.join(os.path.dirname(__file__), "Texts", file)
        out = subprocess.check_output(["grep" "-i", "-o", "-c", "-f", filepath, word])
        try:
            count = int(out)
        except ValueError:
            return -1
        return count


if __name__ == "__main__":
    server = ThreadedServer(WordCountService, port=7777)
    server.start()