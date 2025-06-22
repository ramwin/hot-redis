#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


import time
import unittest

from redis.cluster import RedisCluster
from hot_redis.debounce_task import DebounceInfoTask


class Test(unittest.TestCase):

    def test(self):
        task = DebounceInfoTask(
                client=RedisCluster.from_url("redis://localhost:7000", decode_responses=True),
                key="debounce_task",
                timeout=20,
        )
        task.client.delete("debounce_task")
        task.add_task({"name": "alice", "exam": "math"})
        task.add_task({"name": "alice", "exam": "math"})
        time.sleep(0.1)
        task.add_task({"name": "bob", "exam": "PE"})
        task.add_task({"name": "alice", "exam": "math"})
        self.assertEqual(
            task.pop_tasks(count=2),
            []
        )
        time.sleep(0.1 * 10)
        self.assertEqual(task.pop_tasks(count=2), [])
        time.sleep(0.1 * 9)
        self.assertEqual(
            task.pop_tasks(count=2),
            [{"name": "alice", "exam": "math"}],
        )
        time.sleep(0.1 * 1)
        self.assertEqual(
            task.pop_tasks(count=2),
            [{"name": "bob", "exam": "PE"}],
        )
        time.sleep(0.1 * 20)
        self.assertEqual(task.pop_tasks(count=2), [])

    def test2(self):
        task = DebounceInfoTask(
                client=RedisCluster.from_url("redis://localhost:7000", decode_responses=True),
                key="debounce_task",
                timeout=20,
        )
        task.client.delete("debounce_task")
        task.add_task({"key": "first"}, extra_delay=0.1)
        task.add_task({"key": "second"})
        time.sleep(2)
        tasks = task.pop_tasks(count=2)
        self.assertEqual( tasks, [{"key": "second"}])
        tasks = task.pop_tasks(count=2)
        self.assertEqual( tasks, [])
        time.sleep(0.1)
        tasks = task.pop_tasks(count=2)
        self.assertEqual( tasks, [{"key": "first"}])
