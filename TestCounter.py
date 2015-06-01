import unittest
import math
import time
import RedisCounter as tsCounter


class RedisCounterTest(unittest.TestCase):

    num_of_buckets = 24
    redis_server = {'host': 'localhost', 'port': 6379, 'db': 0}

    def redis_flushdb(self):
        tsCounter.RedisHandler.set_server(self.redis_server)
        tsCounter.RedisHandler.get_connection().flushdb()

    def test_gravity_zero(self):
        """gravity zero, are weights 1?"""
        self.assertEqual(1, tsCounter.exponential_decay(0), "weight != 1")
        self.assertEqual(1, tsCounter.exponential_decay(self.num_of_buckets - 1), "weight != 1")

    def test_gravity_default(self):
        self.assertEqual(1, tsCounter.exponential_decay(0), "weight != 1")
        weight_val = round(pow(self.num_of_buckets, -0.714), 5)
        self.assertEqual(weight_val, tsCounter.exponential_decay(self.num_of_buckets - 1), "wrong weight")

    def test_aggregate_buckets_weight_1(self):
        a = {'1': 1, '2': 5, '3': 3, '4': 1}
        b = {'2': 2, '4': 1, '5': 4}
        c = tsCounter.aggregate_buckets(a, b, 1)
        self.assertEqual(1, c['1'], "wrong aggregate '1'")
        self.assertEqual(7, c['2'], "wrong aggregate '2'")
        self.assertEqual(3, c['3'], "wrong aggregate '3'")
        self.assertEqual(2, c['4'], "wrong aggregate '4'")
        self.assertEqual(4, c['5'], "wrong aggregate '5'")

    def test_aggregate_buckets_weight_3(self):
        a = {'1': 1, '2': 5, '3': 3, '4': 1}
        b = {'2': 2, '4': 1, '5': 4}
        c = tsCounter.aggregate_buckets(a, b, 3)
        self.assertEqual(1, c['1'], "wrong aggregate '1'")
        self.assertEqual(11, c['2'], "wrong aggregate '2'")
        self.assertEqual(3, c['3'], "wrong aggregate '3'")
        self.assertEqual(4, c['4'], "wrong aggregate '4'")
        self.assertEqual(12, c['5'], "wrong aggregate '5'")

    def test_aggregate_buckets_weight_0(self):
        a = {'1': 1, '2': 5, '3': 3, '4': 1}
        b = {'2': 2, '4': 1, '5': 4}
        c = tsCounter.aggregate_buckets(a, b, 0)
        self.assertEqual(1, c['1'], "wrong aggregate '1'")
        self.assertEqual(5, c['2'], "wrong aggregate '2'")
        self.assertEqual(3, c['3'], "wrong aggregate '3'")
        self.assertEqual(1, c['4'], "wrong aggregate '4'")
        self.assertEqual(0, c['5'], "wrong aggregate '5'")

    def test_incr_bucket(self):
        self.redis_flushdb()
        params = tsCounter.TSParams("test", 120, 5, 25, 0, 0)
        ts_count = tsCounter.TSCounterRedis(self.redis_server, params)
        ts_count.incr_count('a', 1.0)
        bucket_id = int(math.ceil(int(time.time()) / params.bucket_time_width / 60))
        key = ts_count.get_bucket_key(bucket_id)
        val = tsCounter.RedisHandler.get_connection().zscore(key, 'a')
        self.assertEqual(1, val, "wrong value for counter")

    def test_bucket_count(self):
        self.redis_flushdb()
        params = tsCounter.TSParams("test", 120, 5, 25, 0, 0)
        ts_count = tsCounter.TSCounterRedis(self.redis_server, params)
        ts_count.incr_count('a', 1)
        ts_count.incr_count('b', 3)
        ts_count.incr_count('c', 5)
        ts_count.incr_count('d', 1)
        bucket_id = int(math.ceil(int(time.time()) / params.bucket_time_width / 60))
        print "current bucket: " + str(bucket_id)
        top2 = ts_count.get_bucket_count(bucket_id, 2)
        print top2
        keys = top2.keys()
        self.assertEqual(keys[0], 'c', "wrong ranking 1st position should be 'c'")
        self.assertEqual(keys[1], 'b', "wrong ranking 2nd position should be 'b'")

    def test_ranking_no_decay(self):
        self.redis_flushdb()
        params = tsCounter.TSParams("", 120, 5, 25, 0, 0)
        bucket_id = int(math.ceil(int(time.time()) / params.bucket_time_width / 60))
        r = tsCounter.RedisHandler.get_connection()
        r.zincrby(bucket_id, 'a', 6)
        r.zincrby(bucket_id, 'b', 4)
        r.zincrby(bucket_id, 'c', 2)
        r.zincrby(bucket_id, 'd', 2)
        r.zincrby(bucket_id - 1, 'a', 1)
        r.zincrby(bucket_id - 1, 'b', 4)
        r.zincrby(bucket_id - 1, 'c', 1)
        r.zincrby(bucket_id - 1, 'd', 3)
        r.zincrby(bucket_id - 2, 'a', 2)
        r.zincrby(bucket_id - 2, 'b', 2)
        r.zincrby(bucket_id - 2, 'c', 4)
        r.zincrby(bucket_id - 2, 'd', 1)

        ts_count = tsCounter.TSCounterRedis(self.redis_server, params)
        rank_list = ts_count.get_ranking(params.total_time_width, 3)
        print rank_list
        self.assertEqual(3, len(rank_list), "some elements misssing in the list?")
        ids = rank_list.keys()
        self.assertEqual('b', ids[0], "b has to be in 1st place")
        self.assertEqual('a', ids[1], "a has to be in 1st place")
        self.assertEqual('c', ids[2], "c has to be in 3rd place")

    def test_ranking_with_decay(self):
        self.redis_flushdb()
        params = tsCounter.TSParams("", 120, 5, 25, 0, tsCounter.exponential_decay)
        bucket_id = int(math.ceil(int(time.time()) / params.bucket_time_width / 60))
        r = tsCounter.RedisHandler.get_connection()
        r.zincrby(bucket_id, 'a', 6)
        r.zincrby(bucket_id, 'b', 4)
        r.zincrby(bucket_id, 'c', 2)
        r.zincrby(bucket_id, 'd', 3)
        r.zincrby(bucket_id - 1, 'a', 1)
        r.zincrby(bucket_id - 1, 'b', 4)
        r.zincrby(bucket_id - 1, 'c', 1)
        r.zincrby(bucket_id - 1, 'd', 2)
        r.zincrby(bucket_id - 2, 'a', 2)
        r.zincrby(bucket_id - 2, 'b', 2)
        r.zincrby(bucket_id - 2, 'c', 4)
        r.zincrby(bucket_id - 2, 'd', 1)

        ts_count = tsCounter.TSCounterRedis(self.redis_server, params)
        rank_list = ts_count.get_ranking(params.total_time_width, 3)
        print rank_list
        self.assertEqual(3, len(rank_list), "some elements misssing in the list?")
        ids = rank_list.keys()
        self.assertEqual('a', ids[0], "a has to be in 1st place")
        self.assertEqual('b', ids[1], "b hast to be in 2nd place")
        self.assertEqual('d', ids[2], "d has to be in 3rd place")

if __name__ == '__main__':
    unittest.main()
