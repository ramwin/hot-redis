#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import random
import time

from typing import Dict, TypeVar, Generic, Union, Optional

from redis import Redis


K = TypeVar('K', bound=Union[str, int, float])
V = TypeVar('V', bound=Union[str, int, float])


class DelayButFastDict(Generic[K, V]):
    """
    This class will read data from redis periodically and keep data in memory
    usage:

        USER_CACHE = DelayButFastDict(Redis(decode_responses=True), key="USER_CACHE", timeout=5)
        USER_CACHE["123"]  # KeyError if not exists
        USER_CACHE["123"] = "user_data"
        USER_CACHE["123"]  # "user_data"

        USER_CACHE = DelayButFastDict(Redis(decode_responses=True), key="USER_CACHE", timeout=5)  
        USER_CACHE["123"]  # "user_data"
    """

    def __init__(self, redis_client=None, key="", timeout=10, startup_init: bool = False):
        """
        params:
            startup_init: load data from redis on instance initialized
        """
        if redis_client is None:
            redis_client = Redis(decode_responses=True)
        if not key:
            raise ValueError("key cannot be empty")
        assert redis_client.get_encoder().decode_responses is True

        self.redis_client = redis_client
        # Use hash tags to ensure both keys are in the same Redis Cluster slot
        self.value_key = f"{{{key}}}:value"
        self.version_key = f"{{{key}}}:version"

        self.timeout = timeout
        self._value: Dict[str, str] = {}

        if startup_init:
            self.expire_at = time.perf_counter() + random.random() * timeout
            self.version = 0
            self.refresh()
        else:
            self.expire_at = time.perf_counter()
            self.version = -1

    def __contains__(self, key: K) -> bool:
        self.refresh_in_need()
        return str(key) in self._value

    def __getitem__(self, key: K) -> V:
        self.refresh_in_need()
        str_key = str(key)
        if str_key not in self._value:
            raise KeyError(key)
        return self._value[str_key]  # type: ignore

    def __setitem__(self, key: K, value: V) -> None:
        str_key = str(key)
        str_value = str(value)
        # Execute Redis operations first
        self.redis_client.pipeline()\
                .hset(self.value_key, str_key, str_value)\
                .incr(self.version_key)\
                .execute()
        # Update local value only after Redis operation succeeds
        self._value[str_key] = str_value
        # Local version will be updated on next refresh

    def __delitem__(self, key: K) -> None:
        str_key = str(key)
        # Execute Redis operations first
        self.redis_client.pipeline()\
                .hdel(self.value_key, str_key)\
                .incr(self.version_key)\
                .execute()
        # Update local value only after Redis operation succeeds
        if str_key in self._value:
            del self._value[str_key]
        # Local version will be updated on next refresh

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
                .hgetall(self.value_key)\
                .execute()
        self.version = int(version or 0)
        self._value = value or {}

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        self.refresh_in_need()
        str_key = str(key)
        if str_key in self._value:
            return self._value[str_key]  # type: ignore
        return default

    def setdefault(self, key: K, default: V) -> V:
        str_key = str(key)
        if str_key not in self:
            self[key] = default
        return self[key]

    def pop(self, key: K, default: Optional[V] = None) -> Optional[V]:
        str_key = str(key)
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            if default is not None:
                return default
            raise

    def update(self, *args, **kwargs) -> None:
        if args:
            if len(args) > 1:
                raise TypeError(f"update expected at most 1 argument, got {len(args)}")
            other = args[0]
            if hasattr(other, "keys"):
                for key in other.keys():
                    self[key] = other[key]
            else:
                for key, value in other:
                    self[key] = value
        for key, value in kwargs.items():
            self[key] = value

    def keys(self):
        self.refresh_in_need()
        return self._value.keys()

    def values(self):
        self.refresh_in_need()
        return self._value.values()

    def items(self):
        self.refresh_in_need()
        return self._value.items()

    def clear(self) -> None:
        # Execute Redis operations first
        self.redis_client.pipeline()\
                .delete(self.value_key)\
                .incr(self.version_key)\
                .execute()
        # Update local value only after Redis operation succeeds
        self._value.clear()
        # Local version will be updated on next refresh

    def __iter__(self):
        self.refresh_in_need()
        return iter(self._value)

    def __len__(self):
        self.refresh_in_need()
        return len(self._value)

    def __str__(self):
        self.refresh_in_need()
        if len(self._value) <= 100:
            return f"DelayButFastDict:{self.value_key}:{self.version_key}: {self._value}"
        return f"DelayButFastDict:{self.value_key}:{self.version_key}: too many values..."

    def __repr__(self):
        self.refresh_in_need()
        if len(self._value) <= 100:
            return f"DelayButFastDict:{self.value_key}:{self.version_key}: {self._value}"
        return f"DelayButFastDict:{self.value_key}:{self.version_key}: too many values..."
