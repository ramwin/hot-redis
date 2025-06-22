#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


import datetime
import time
import json
import warnings

from typing import (
    cast,
    Generic,
    List,
    Union,
    TypedDict,
    TypeVar,
)

from redis import Redis
from redis.cluster import RedisCluster


class DebounceTask:
    DELTA = int(datetime.datetime(2025, 1, 1, 0, 0, 0).timestamp())

    def __init__(self, client: Redis, key: str, timeout: int):
        """
        timeout: 1means 0.1s
        """
        warnings.warn(DeprecationWarning("DebounceTask should be replaced with DebounceInfoTask, this will be removed in 1.0.0"))
        self.client = client
        self.key = key
        self.timeout = timeout

    def get_time(self) -> int:
        return int((time.time() - self.DELTA) * 10)

    def add_task(self, taskid: str):
        self.client.zadd(self.key, {taskid: self.get_time()}, nx=True)

    def pop_tasks(self, max_wait: int=0) -> List[str]:
        """
        params:
            max_wait: 1 means 0.1 seconds
        """
        tasks = self._pop_tasks()
        if tasks or (not max_wait):
            return tasks
        first_task = self.client.zrange(self.key, 0, 0, withscores=True)
        if not first_task:
            time.sleep(max_wait * 0.1)
            return []
        add_time = first_task[0][1]
        now = self.get_time()
        need_wait = max(0, self.timeout - (now - add_time))
        if need_wait <= max_wait:
            time.sleep(need_wait / 10)
            return self._pop_tasks()
        return self._pop_tasks()

    def _pop_tasks(self) -> List[str]:
        now = self.get_time()
        delete_before = now - self.timeout

        with self.client.lock(self.key + ":lock", timeout=5):
            taskids, _ = self.client.pipeline()\
                .zrangebyscore(self.key, min=0, max=delete_before)\
                .zremrangebyscore(self.key, min=0, max=delete_before)\
                .execute()
            return taskids


class Info(TypedDict):
    remain_cnt: int
    overtime_cnt: int


T = TypeVar("T")


class DebounceInfoTask(Generic[T]):
    """compare to debouncetask, I accept dict as input"""
    DELTA = int(datetime.datetime(2025, 1, 1, 0, 0, 0).timestamp())

    def __init__(self, client: Union[Redis, RedisCluster], key: str, timeout: int):
        """
        timeout: 1means 0.1s
        """
        self.client = client
        self.key = key
        self.timeout = timeout

    def get_time(self) -> int:
        return int((time.time() - self.DELTA) * 10)

    def add_task(self, task_info: T, extra_delay: float=0):
        """
        params:
            extra_delay: wait extra seconds(float) then normal task
        """
        taskid = json.dumps(task_info, ensure_ascii=False, sort_keys=True)
        self.client.zadd(self.key, {taskid: self.get_time() + extra_delay}, nx=True)

    def pop_tasks(self, max_wait: int=0, count: int=100) -> List[T]:
        """
        params:
            max_wait: 1 means 0.1 seconds
            count: max size of task
        """
        tasks = self._pop_tasks(count)
        if tasks or (not max_wait):
            return tasks
        first_task = self.client.zrange(self.key, 0, 0, withscores=True)
        if not first_task:
            time.sleep(max_wait * 0.1)
            return []
        add_time = first_task[0][1]
        now = self.get_time()
        need_wait = max(0, self.timeout - (now - add_time))
        if need_wait <= max_wait:
            time.sleep(need_wait / 10)
            return self._pop_tasks(count)
        return self._pop_tasks(count)

    def get_tasks(self, count: int=100) -> List[T]:
        """get task without pop"""
        now = self.get_time()
        delete_before = now - self.timeout
        taskids = self.client.zrangebyscore(
            self.key,
            min=0, max=delete_before,
            start=0, num=count,
        )
        return [
                cast(T, json.loads(cast(str, taskid)))
                for taskid in taskids
        ]

    def _pop_tasks(self, count: int) -> List[T]:
        now = self.get_time()
        delete_before = now - self.timeout
        last_obj = self.client.zrange(self.key, count-1, count-1, withscores=True)
        if last_obj and last_obj[0][1] < delete_before:
            # exist too many task
            taskids = self.client.zpopmin(self.key, count)
            return [
                    json.loads(taskid[0])
                    for taskid in taskids
            ]
        taskids, _ = self.client.pipeline()\
            .zrangebyscore(self.key, min=0, max=delete_before)\
            .zremrangebyscore(self.key, min=0, max=delete_before)\
            .execute()
        return [
                cast(T, json.loads(cast(str, taskid)))
                for taskid in taskids
        ]

    def get_info(self) -> Info:
        """
        return current info
            remain_cnt: the task in redis
            overtime_cnt: the task need to be handled(created before timeout) in redis
        """
        delete_before = self.get_time() - self.timeout
        return {
                "overtime_cnt": self.client.zcount(self.key, min=0, max=delete_before),
                "remain_cnt": self.client.zcard(self.key),
        }

    def __str__(self):
        return f"DebounceInfoTask:{self.key}"
