#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import json
from redis import Redis
from typing import (
        List,
        Optional,
        Tuple,
        )


class MultiWaitTask:
    """
    wait multi keys for handler
    """

    def __init__(self, client: Redis, keys: List[str], batch_size: int = 10):
        self.client = client
        self.keys = keys
        self.batch_size = batch_size

    def get_tasks(self) -> Optional[Tuple[str, List[dict]]]:
        result = self.client.blmpop(
                10,
                len(self.keys),
                *self.keys,
                direction="LEFT",
                count=self.batch_size,
        )
        if not result:
            return None
        return result[0], [
                json.loads(i)
                for i in result[1]
        ]

    def add_task(self, key, info: dict):
        self.client.rpush(key, json.dumps(info, ensure_ascii=False, indent=4))
