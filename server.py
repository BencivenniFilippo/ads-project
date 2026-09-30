import rpyc
from rpyc.utils.server import ThreadedServer


class WordCountService(rpyc.Service):
    def exposed_word_count(self):
        return


if __name__ == "__main__":
    server = ThreadedServer(MyService, port=7777)
    server.start()