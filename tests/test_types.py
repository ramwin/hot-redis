#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


import time
import unittest
import logging

from hot_redis.fast_set import DelayButFastSet
from redis import Redis

from multiprocessing import Pool


logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
LOGGER = logging.getLogger(__name__)


class Test(unittest.TestCase):

    @staticmethod
    def inner_test_performance(taskid: int):
        size = 10000
        Redis().delete("set_fastkey:value")
        Redis().delete("set_fastkey:version")
        Redis().delete("set_direct")
        a = DelayButFastSet(redis_client=Redis(decode_responses=True), key="set_fastkey", timeout=5)
        for i in range(0, size, 2):
            a.add(str(i))

        start = time.perf_counter()
        b = DelayButFastSet(redis_client=Redis(decode_responses=True), key="set_fastkey", timeout=5)
        cnt = 0
        for j in range(0, size, 3):
            if str(j) in b:
                cnt += 1
        end = time.perf_counter()
        LOGGER.info("task %d use DelayButFastSet: %f, result: %d", taskid, end - start, cnt)

        client = Redis(decode_responses=True)
        for i in range(0, size, 2):
            client.sadd("set_direct", str(i))

        start = time.perf_counter()
        cnt = 0
        for j in range(0, size, 3):
            if client.sismember("set_direct", str(j)):
                cnt += 1
        end = time.perf_counter()
        LOGGER.info("task %d user redis directly: %f, result: %d", taskid, end - start, cnt)

    def test_performance(self):
        task_cnt = 4
        with Pool(task_cnt) as p:
            p.map(self.inner_test_performance, range(task_cnt))

    def test_diff(self):
        a = DelayButFastSet(key="fast-set")
        a.update("a", "b")
        b = DelayButFastSet(key="fast-set2")
        b.update("a", "c")
        self.assertEqual(a - b, {"b"})
        self.assertEqual({"a", "c"} - a._value, {"c"})
        print(a)
        print(b)
