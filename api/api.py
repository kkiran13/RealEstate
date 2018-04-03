from flask import Flask
import redis
from load_cache_data import LoadCacheData

app = Flask(__name__)

redis_pool = redis.ConnectionPool(host='redisservice',
                                  port=6379,
                                  db=0)
redis_conn = redis.Redis(connection_pool=redis_pool)


@app.route("/")
def home():
    return "Welcome to customer representative app!!!"


@app.route("/refresh")
def load():
    try:
        LoadCacheData().process()
        return 'Completed cache refresh'
    except Exception as e:
        return 'Error refreshing cache'


@app.route("/client/<client_name>")
def get_client_activity(client_name):
    res = redis_conn.get(client_name)
    return res.decode('zlib') if res is not None else '{}'


@app.route("/address/<property_address>")
def get_address_info(property_address):
    res = redis_conn.get(property_address)
    return res.decode('zlib') if res is not None else '{}'


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')

