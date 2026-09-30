import rpyc
from rpyc.utils.server import ThreadedServer


class WordCountService(rpyc.Service):
    def exposed_word_count(self):
        return


if __name__ == "__main__":
    server = ThreadedServer(WordCountService, port=7777)
    server.start()