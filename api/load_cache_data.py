import psycopg2
import redis
import json
from collections import defaultdict


class LoadCacheData:
    """
    Helper task for API so that responses to requests from customer tool have low latency
    Batch Job which runs on a schedule. Performs below operations:
        * Loads redis cache with data for buyer and seller - Counts of bought/sold properties
        * Loads redis cache with transactions per address/property
    """

    BUYER_QUERY = """
    SELECT buyer, count(1)
    FROM public.transactions
    WHERE buyer IS NOT NULL
    GROUP BY 1
    """

    SELLER_QUERY = """
    SELECT seller, count(1)
    FROM public.transactions
    WHERE seller IS NOT NULL
    GROUP BY 1
    """

    ADDRESS_QUERY = """
    SELECT property_address, COUNT(1)
    FROM public.transactions
    WHERE property_address IS NOT NULL
    GROUP BY 1
    """

    def __init__(self):
        self.redshift_conn = self.init_redshift_connection()
        self.redis_conn = self.init_redis_connection()

    @staticmethod
    def init_redshift_connection():
        """
        :return: Redshift connection
        """
        conn = psycopg2.connect(host='targetdb',
                                dbname='postgres',
                                user='postgres',
                                password='password',
                                port=5432
                                )
        return conn

    @staticmethod
    def init_redis_connection():
        """
        :return: Redis connection
        """
        pool = redis.ConnectionPool(host='redisservice',
                                    port=6379,
                                    db=0
                                    )
        conn = redis.Redis(connection_pool=pool)
        return conn

    def process(self):
        """
        Loads redis with data in redshift
        :return: None
        """
        try:
            redshift_cursor = self.redshift_conn.cursor()

            client_map = defaultdict(lambda: {"sold": 0, "bought": 0})
            address_map = defaultdict(lambda: {"flips": 0})

            redshift_cursor.execute(self.SELLER_QUERY)
            rows = redshift_cursor.fetchall()
            for seller, count in rows:
                client_map[seller]['sold'] = count

            redshift_cursor.execute(self.BUYER_QUERY)
            rows = redshift_cursor.fetchall()
            for buyer, count in rows:
                client_map[buyer]['bought'] = count

            redshift_cursor.execute(self.ADDRESS_QUERY)
            rows = redshift_cursor.fetchall()
            for address, count in rows:
                address_map[address]['flips'] = count

            print address_map

            # Load to redis
            redis_pipeline = self.redis_conn.pipeline()
            self.execute_pipeline(records=client_map, pipeline=redis_pipeline)
            self.execute_pipeline(records=address_map, pipeline=redis_pipeline)

            redshift_cursor.close()

        except Exception as e:
            print (e)

        finally:
            if self.redshift_conn is not None:
                self.redshift_conn.close()

    @staticmethod
    def execute_pipeline(records, pipeline):
        """
        load data into redis from dict
        :param records: dict containing buyer/seller name as key and value as {"sold": X, "bought": X}
        :param pipeline: redis pipeline
        :return: None
        """
        for client, activity in records.items():
            pipeline.set(
                name=client,
                value=json.dumps(activity).encode('zlib'),
                ex=60 * 60 * 24
            )

        pipeline.execute()

    def retrieve_from_redis(self):
        """
        Read keys and values from redis
        :return: None
        """
        for key in self.redis_conn.scan_iter():
            print ("%s -----------> %s" % (key, self.redis_conn.get(key).decode('zlib')))
