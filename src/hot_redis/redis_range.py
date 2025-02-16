#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


from redis import Redis


class RedisRange:
    """
    In many scenario, you want to handle a task from start to end;
    e.g.
        save all database value to cache from id=1 to largest

        updated_range = RedisRange(redis_client=Reids(decode_responses=True), key="student_name_update_range")
        student_names = DelayButFastSet(redis_client=Reids(decode_responses=True), key="student_name_set")

        when user created:
            student_names.add(user.name)
            for old_user_id in range(updated_range.get_max() + 1, user.id - 1):
                student_names.add(User(id=old_user_id).name)
            updated_range.upper_to(user.id)

            updated_min = updated_range.get_min()
            to_update_min = updated_min - 10
            for old_user_id in range(to_update_min, updated_min):
                student_names.add(User(id=old_user_id).name)
            updated_range.lower_to(to_update_min)
    """

    def __init__(self, redis_client: Redis, key: str):
        assert redis_client.get_encoder().decode_responses is True
        self.redis_client = redis_client
        self.min_key = f"{key}:min"  # min value include
        self.max_key = f"{key}:max"  # max value include
        self.lock_key = f"{key}:lock"
        self.pending_key = f"{key}:pending"
        self.inited = self.redis_client.exists(self.max_key)

    def get_min(self):
        return int(self.redis_client.get(self.min_key) or 0)

    def get_max(self):
        return int(self.redis_client.get(self.max_key) or 0)

    def upper_to(self, stop: int):
        """
        increate max to stop 
        """
        if not self.inited:
            self.init_with(stop)
        self.redis_client.set(self.max_key, stop)

    def lower_to(self, start: int):
        if not self.inited:
            self.init_with(start)
        self.redis_client.set(self.min_key, start)

    def init_with(self, value: int):
        with self.redis_client.lock(self.lock_key):
            self.redis_client.pipeline()\
                    .set(self.max_key, value)\
                    .set(self.min_key, value)\
                    .execute()
            self.inited = True

    def add(self, 
