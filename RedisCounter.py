__author__ = 'vruizext'

import redis
import math
import time
from operator import itemgetter


def exponential_decay(t, gravity=0.714):
    """
    provides a vector with weights exponentially decaying, based on HN ranking algorithm:

        weights[t] = 1 / (t + 1)^gravity,

    The higher the t, the lower the weight
    The higher the gravity, the faster the weight decreases for higher t values
    With default value 0.714, weight = 0.1 for t = 24
    """
    return round(pow(t + 1, -gravity), 5)


def aggregate_buckets(a, b, w=1):
    """
    aggregate elements from dictionary b to dictionary a, multiplying first by a factor w
    the counts of elements of b
    :param a: dictionary containing pairs (key, val)
    :param b: dictionary containing pairs (key, val)
    :param w: weight applied to count values of elements in b
    :return:
    """
    for key, val in b.iteritems():
        a[id] = a.get(key, 0) + val * w
    return a


class TSParams:
    """
    This class is just a place holder for config params
    """
    def __init__(self, key_prefix, total_time_width, bucket_time_width, bucket_size, min_count, decay):
        self.prefix = key_prefix
        self.total_time_width = total_time_width
        self.bucket_time_width = bucket_time_width
        self.bucket_size = bucket_size
        self.total_buckets = math.ceil(total_time_width / bucket_time_width)
        self.min_count = min_count
        self.decay = decay


class RedisHandler:
    """
    handles redis connections
    """

    server = {}

    @classmethod
    def set_server(cls, server):
        cls.server = server

    @classmethod
    def get_pool(cls):
        try:
            pool = cls.pool
        except AttributeError:
            pool = redis.ConnectionPool(host=cls.server['host'], port=cls.server['port'], db=cls.server['db'])
        return pool

    @classmethod
    def get_connection(cls):
        return redis.Redis(connection_pool=cls.get_pool())

    @classmethod
    def get_pipe(cls):
        return cls.get_connection().pipeline()


class TSCounterBase:
    """
    base class for Time Series Counters
    Allows to track counts of things in intervals i.e. buckets of configurable width
    """
    def __init__(self, config):
        self.prefix = config.prefix
        self.total_time_width = config.total_time_width
        self.bucket_time_width = config.bucket_time_width
        self.bucket_size = config.bucket_size
        self.total_buckets = config.total_buckets
        self.min_count = config.min_count
        self.decay = config.decay

    def get_bucket_key(self, bucket_id=None):
        if bucket_id is None:
            bucket_id = int(math.ceil(int(time.time()) / self.bucket_time_width / 60))
        return self.prefix + str(bucket_id)

    def get_ranking(self, time_width, how_many):
        pass

    def incr_count(self, member_id, count):
        pass


class TSCounterRedis(TSCounterBase):
    """
    Time series counter which uses redis to store data
    """

    def __init__(self, server, config):
        RedisHandler.set_server(server)
        self.redis = RedisHandler.get_connection()
        TSCounterBase.__init__(self, config)

    def incr_count(self, member_id, count):
        """
        Increments the count of member_id in the current bucket by count
        :param member_id: identifier of the member / item / whatever is being accounted
        :param count: increase count by this amount
        :return: the new count of the member
        """
        key = self.get_bucket_key()
        return self.redis.zincrby(key, member_id, count)

    def get_bucket_count(self, bucket_id, how_many):
        """
        get counter values for an arbitrary bucket
        :param bucket_id: identifier for this bucket
        :param how_many: how many items to return (top N)
        :return: dictionary containing tuples (id, count)
        """
        key = self.get_bucket_key(bucket_id)
        return dict(self.redis.zrevrange(key, 0, how_many - 1, True))

    def get_ranking(self, time_width, how_many):
        """
        aggregates all counters within a given time range, from the current time backwards
        :param time_width: time range which is being aggregated, in minutes
        :param how_many:  how many items to return
        :return: a dictionary containing tuples (id, count)
        """
        total_count = {}
        num_of_buckets = int(math.ceil(time_width / self.bucket_time_width))
        first_bucket = int(math.ceil(int(time.time()) / self.bucket_time_width / 60))

        for bucket_id in xrange(first_bucket, first_bucket - num_of_buckets, -1):
            bucket_count = self.get_bucket_count(bucket_id, 3 * how_many)
            decay = self.decay(first_bucket - bucket_id)
            total_count = aggregate_buckets(total_count, bucket_count, decay)

        total_count = sorted(total_count.items(), key=itemgetter(2), reverse=True)
        return total_count[:how_many]



