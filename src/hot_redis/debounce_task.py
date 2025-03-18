#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


import datetime
import time

from typing import List
from redis import Redis


class DebounceTask:
    DELTA = int(datetime.datetime(2025, 1, 1, 0, 0, 0).timestamp())

    def __init__(self, client: Redis, key: str, timeout: int):
        """
        timeout: 1means 0.1s
        """
        self.client = client
        self.key = key
        self.timeout = timeout

    def get_time(self) -> int:
        return int((time.time() - self.DELTA) * 10)

    def add_task(self, taskid: str):
        self.client.zadd(self.key, {taskid: self.get_time()}, nx=True)

    def pop_tasks(self) -> List[str]:
        now = self.get_time()
        delete_before = now - self.timeout

        with self.client.lock(self.key + ":lock", timeout=5):
            taskids, _ = self.client.pipeline()\
                .zrangebyscore(self.key, min=0, max=delete_before)\
                .zremrangebyscore(self.key, min=0, max=delete_before)\
                .execute()
            return taskids
