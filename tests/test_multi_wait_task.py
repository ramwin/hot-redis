#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import logging
import unittest
from redis import Redis
from redis.cluster import RedisCluster

from hot_redis.multi_wait_task import MultiWaitTask


LOGGER = logging.getLogger(__name__)


class Test(unittest.TestCase):

    def test1(self):
        task = MultiWaitTask(
                client=Redis(decode_responses=True),
                keys=["list1", "list2"], batch_size=3
        )
        task.add_task("list1", ["list1task_a"])
        task.add_task("list1", ["list1task_b"])
        task.add_task("list2", ["list2task"])
        LOGGER.error(task.get_tasks())
        LOGGER.error(task.get_tasks())
        LOGGER.error(task.get_tasks())

    def test2(self):
        task = MultiWaitTask(
                client=RedisCluster.from_url(
                    "redis://localhost:7000",
                    decode_responses=True),
                keys=["list1", "list2"], batch_size=3
        )
        task.add_task("list1", ["list1task_a"])
        task.add_task("list1", ["list1task_b"])
        task.add_task("list2", ["list2task"])
        LOGGER.error(task.get_tasks())
        LOGGER.error(task.get_tasks())
        LOGGER.error(task.get_tasks())
