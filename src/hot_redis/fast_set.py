#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import random
import time

from redis import Redis


class DelayButFastSet:
    """
    this class will read data from redis periodly and keep data in memory
    usage:

        WATCHING_USERS = DelayBuyFastSet(Redis(), key="WATCHING_USERS", timeout=5)
        "123" in WATCHING_USERS  # False
        WATCHING_USERS.add("123")
        "123" in WATCHING_USERS  # True

        WATCHING_USERS = DelayBuyFastSet(Redis(), key="WATCHING_USERS", timeout=5)
        "123" in WATCHING_USERS  # True
    """

    def __init__(self, redis_client, key, timeout):
        assert redis_client.get_encoder().decode_responses is True

        self.redis_client = redis_client
        self.value_key = f"{key}:value"
        self.version_key = f"{key}:version"

        self.timeout = timeout
        self.expire_at = time.perf_counter() + random.random() * timeout

        self._value = set()
        self.version = 0
        self.refresh()

    def __contains__(self, value):
        self.refresh_in_need()
        return str(value) in self._value

    def refresh_in_need(self):
        if time.perf_counter() < self.expire_at:
            return
        self.expire_at = time.perf_counter() + self.timeout
        if int(self.redis_client.get(self.version_key) or 0) == self.version:
            return
        self.refresh()

    def refresh(self):
        self._value = self.redis_client.smembers(self.value_key)
        self.version = self.redis_client.incr(self.version_key)

    def add(self, value: str):
        assert isinstance(value, str)
        self._value.add(value)
        added, version = self.redis_client.pipeline()\
                .sadd(self.value_key, value)\
                .incr(self.version_key)\
                .execute()

    def discard(self, value: str):
        assert isinstance(value, str)
        self._value.discard(value)
        self.redis_client.pipeline()\
                .srem(self.value_key, value)\
                .incr(self.version_key)\
                .execute()

    def __iter__(self):
        self.refresh_in_need()
        return self._value.__iter__()
