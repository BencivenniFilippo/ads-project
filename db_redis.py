import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)


r.set('foo', 'bar')
# Connect to the db anc check if the value is set. Use local host 127.0.0 and port 6379