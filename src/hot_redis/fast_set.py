#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import random
import time

from typing import Set, TypeVar, Generic, Union

from redis import Redis


T = TypeVar('T', bound=Union[str, int, float])


class DelayButFastSet(Generic[T]):
    """
    this class will read data from redis periodly and keep data in memory
    usage:

        WATCHING_USERS = DelayBuyFastSet(Redis(decode_responses=True), key="WATCHING_USERS", timeout=5)
        "123" in WATCHING_USERS  # False
        WATCHING_USERS.add("123")
        "123" in WATCHING_USERS  # True

        WATCHING_USERS = DelayBuyFastSet(Redis(decode_responses=True), key="WATCHING_USERS", timeout=5)
        "123" in WATCHING_USERS  # True
    """

    def __init__(self, redis_client=None, key="", timeout=10, startup_init: bool = False, version: int=1):
        """
        params:
            startup_init: load data from redis on instance initialized
            version: in version1, the key may be in different slot
        """
        if redis_client is None:
            redis_client = Redis(decode_responses=True)
        if not key:
            raise ValueError
        assert redis_client.get_encoder().decode_responses is True

        self.redis_client = redis_client
        # Use hash tags to ensure both keys are in the same Redis Cluster slot
        if version == 1:
            self.value_key = f"{key}:value"
            self.version_key = f"{key}:version"
        else:
            self.value_key = f"{{{key}}}:value"
            self.version_key = f"{{{key}}}:version"

        self.timeout = timeout
        self._value: Set[str] = set()

        if startup_init:
            self.expire_at = time.perf_counter() + random.random() * timeout
            self.version = 0
            self.refresh()
        else:
            self.expire_at = time.perf_counter()
            self.version = -1

    def __contains__(self, value: T) -> bool:
        self.refresh_in_need()
        return str(value) in self._value

    def refresh_in_need(self) -> None:
        if time.perf_counter() < self.expire_at:
            return
        self.expire_at = time.perf_counter() + self.timeout
        if int(self.redis_client.get(self.version_key) or 0) == self.version:
            return
        self.refresh()

    def refresh(self) -> None:
        version, value = self.redis_client.pipeline()\
                .get(self.version_key)\
                .smembers(self.value_key)\
                .execute()
        self.version = int(version or 0)
        self._value = value or set()

    def add(self, value: T) -> None:
        str_value = str(value)
        self._value.add(str_value)
        added, new_version = self.redis_client.pipeline()\
                .sadd(self.value_key, str_value)\
                .incr(self.version_key)\
                .execute()
        self.version = new_version

    def discard(self, value: T) -> None:
        str_value = str(value)
        self._value.discard(str_value)
        _, new_version = self.redis_client.pipeline()\
                .srem(self.value_key, str_value)\
                .incr(self.version_key)\
                .execute()
        self.version = new_version

    remove = discard

    def update(self, *values: T) -> None:
        str_values = [str(value) for value in values]
        self._value.update(str_values)
        if values:
            _, new_version = self.redis_client.pipeline()\
                    .sadd(self.value_key, *str_values)\
                    .incr(self.version_key)\
                    .execute()
            self.version = new_version

    def __iter__(self):
        self.refresh_in_need()
        return self._value.__iter__()

    def __str__(self):
        self.refresh_in_need()
        if len(self._value) <= 100:
            return f"DelayButFastSet:{self.value_key}:{self.version_key}: {self._value}"
        return f"DelayButFastSet:{self.value_key}:{self.version_key}: too many values..."

    def __repr__(self):
        self.refresh_in_need()
        if len(self._value) <= 100:
            return f"DelayButFastSet:{self.value_key}:{self.version_key}: {self._value}"
        return f"DelayButFastSet:{self.value_key}:{self.version_key}: too many values..."

    def __sub__(self, target):
        self.refresh_in_need()
        if isinstance(target, DelayButFastSet):
            target.refresh_in_need()
            return self._value - target._value
        return self._value - target

    def __len__(self):
        self.refresh_in_need()
        return len(self._value)
