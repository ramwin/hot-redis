#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import json
import random

from typing import (
        List,
        Optional,
        Tuple,
        Union,
        )

from redis import Redis
from redis.cluster import RedisCluster



class MultiWaitTask:
    """
    wait multi keys for handler
    """

    def __init__(self, client: Union[Redis, RedisCluster], keys: List[str], batch_size: int = 10):
        self.client = client
        self.keys = keys
        self.batch_size = batch_size

    def get_tasks(self) -> Optional[Tuple[str, List[dict]]]:
        if isinstance(self.client, Redis):
            result = self.client.blmpop(
                    10,
                    len(self.keys),
                    *self.keys,
                    direction="LEFT",
                    count=self.batch_size,
            )
        else:
            key_result = None
            for key in random.sample(self.keys, len(self.keys)):
                key_result = self.client.lpop(key, self.batch_size)
                if not key_result:
                    continue
                else:
                    break
            if not key_result:
                return None
            return key, [
                    json.loads(i)
                    for i in key_result
            ]
        if not result:
            return None
        return result[0], [
                json.loads(i)
                for i in result[1]
        ]

    def add_task(self, key, info: dict):
        self.client.rpush(key, json.dumps(info, ensure_ascii=False, indent=4))
