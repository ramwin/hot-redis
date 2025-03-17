#!/usr/bin/env python3
# -*- coding: utf-8 -*-



from redis import Redis


class UniqueId:
    """
    When you have many databases, you may want to create an same uniqueid for a unique id.

        e.g.
            there were three databases named: students_info, students_activity, students_history, both shared the same table: students
            when you create a new student, you want to retrieve the student's id.

            unique_id = UniqueId(client=Redis(decode_respones=True), klass="students")
            unique_id.get_or_create("student's id card No.1") => 3 unique
    """

    def __init__(self, client: Redis, klass: str):
        self.client = client
        self.klass = klass
        self.value_key = klass + ":auto"

    def get_or_create(self, key: str) -> int:
        redis_key = self._get_redis_key(key)
        if (created := self.client.get(redis_key)):
            return int(created)
        newid = self.client.incr(self.value_key)
        valid = self.client.set(redis_key, newid, nx=True)
        if valid:
            return newid
        return int(self.client.get(redis_key))

    def set(self, key: str, id: int):
        """override the int for a key"""
        redis_key = self._get_redis_key(key)
        self.client.set(redis_key, id)

    def incr_to(self, id: int):
        self.client.set(self.value_key, id)

    def _get_redis_key(self, key: str):
        return self.klass + ":id:" + key
