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

    def get_min_max(self) -> (int, int):
        assert self.inited
        a, b = self.redis_client.pipeline()\
                .get(self.min_key)\
                .get(self.max_key)\
                .execute()
        return int(a), int(b)

    def get_min(self) -> int:
        assert self.inited
        return int(self.redis_client.get(self.min_key))

    def get_max(self) -> int:
        assert self.inited
        return int(self.redis_client.get(self.max_key))

    def upper_to(self, stop: int) -> None:
        """
        increate max to stop 
        the value in (max: stop) will be added into pending
        """
        if not self.inited:
            self.init_with(stop)
        current_min, current_max = self.get_min_max()
        if stop < current_min:
            raise ValueError
        if stop <= current_max:
            return
        if stop == current_max + 1:
            self.redis_client.set(self.max_key, stop)
            return
        pending_ids = range(current_max+1, stop)
        self.redis_client.pipeline()\
            .set(self.max_key, stop)\
            .sadd(self.pending_key, *pending_ids)\
            .execute()

    def lower_to(self, start: int) -> None:
        """
        decrease min to start
        the value in [start: min) will be added into pending
        """
        if not self.inited:
            self.init_with(start)
        current_min, current_max = self.get_min_max()
        if start > current_max:
            raise ValueError
        elif start >= current_min:
            return
        pending_ids = range(start, current_min)
        self.redis_client.pipeline()\
                .set(self.min_key, start)\
                .sadd(self.pending_key, *pending_ids)\
                .execute()

    def init_with(self, value: int):
        with self.redis_client.lock(self.lock_key):
            if self.redis_client.exists(self.max_key):
                self.inited = True
                return
            self.redis_client.pipeline()\
                    .set(self.max_key, value)\
                    .set(self.min_key, value)\
                    .execute()
            self.inited = True

    def reset(self):
        self.redis_client.pipeline()\
                .delete(self.min_key)\
                .delete(self.max_key)\
                .delete(self.lock_key)\
                .delete(self.pending_key)\
                .execute()
