import os
import rpyc
import logging

client_name = os.getenv("CLIENT_NAME", "client")
logging.basicConfig(
    level=logging.WARNING,
    format=f'%(asctime)s - [{client_name}] - %(levelname)s - %(message)s',
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(client_name)


conn = rpyc.connect(os.getenv('LOAD_BALANCER_HOST'), int(os.getenv('LOAD_BALANCER_PORT', "7777")))

for i in range(5):
    ans = conn.root.exposed_word_count("John_Wick.txt", "John")
    print("the word appears " + str(ans) + " times")

conn.close()