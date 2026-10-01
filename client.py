import rpyc
import os

conn = rpyc.connect(os.getenv('LOAD_BALANCER_HOST'), int(os.getenv('LOAD_BALANCER_PORT', 7777)))
ans = conn.root.word_count("test.txt", "test")
print(ans)
conn.close()