#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


from unittest import TestCase

from redis import Redis

from hot_redis import RedisRange


class Test(TestCase):

    def test(self):
        redis_range_a = RedisRange(
                redis_client=Redis(decode_responses=True),
                key="range",
        )
        redis_range_a.reset()
        redis_range_a = RedisRange(
                redis_client=Redis(decode_responses=True),
                key="range",
        )
        redis_range_b = RedisRange(
                redis_client=Redis(decode_responses=True),
                key="range",
        )
        redis_range_a.upper_to(100)
        redis_range_c = RedisRange(
                redis_client=Redis(decode_responses=True),
                key="range",
        )
        self.assertEqual(redis_range_a.redis_client.smembers(redis_range_a.pending_key), set())
        self.assertEqual(redis_range_a.get_min(), 100)
        self.assertEqual(redis_range_a.get_max(), 100)
        redis_range_b.upper_to(101)
        self.assertEqual(redis_range_b.redis_client.smembers(redis_range_a.pending_key), set())
        self.assertEqual(redis_range_b.get_min(), 100)
        self.assertEqual(redis_range_b.get_max(), 101)
        redis_range_a.upper_to(101)
        self.assertEqual(redis_range_b.redis_client.smembers(redis_range_a.pending_key), set())
        self.assertEqual(redis_range_b.get_min(), 100)
        self.assertEqual(redis_range_b.get_max(), 101)
        with self.assertRaises(ValueError):
            redis_range_a.lower_to(103)
        redis_range_a.upper_to(103)
        self.assertEqual(redis_range_c.redis_client.smembers(redis_range_a.pending_key), {'102'})
        self.assertEqual(redis_range_c.get_min(), 100)
        self.assertEqual(redis_range_c.get_max(), 103)
        redis_range_b.lower_to(99)
        self.assertEqual(redis_range_a.redis_client.smembers(redis_range_a.pending_key), {'99', '102'})
        self.assertEqual(redis_range_a.get_min(), 99)
        self.assertEqual(redis_range_a.get_max(), 103)
        redis_range_c.lower_to(97)
        self.assertEqual(redis_range_a.redis_client.smembers(redis_range_a.pending_key), {'97', '98', '99', '102'})
        self.assertEqual(redis_range_a.get_min(), 97)
        self.assertEqual(redis_range_a.get_max(), 103)
