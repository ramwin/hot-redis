#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


import time
import unittest
import logging
from typing import List

from hot_redis.fast_set import DelayButFastSet
from redis import Redis

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
LOGGER = logging.getLogger(__name__)


class TestFastSetGeneric(unittest.TestCase):

    def setUp(self):
        """每个测试前清理 Redis 数据"""
        self.redis_client = Redis(decode_responses=True)
        self.redis_client.delete("test_set:value")
        self.redis_client.delete("test_set:version")
        self.redis_client.delete("test_set2:value")
        self.redis_client.delete("test_set2:version")

    def test_string_generic(self):
        """测试字符串类型的泛型支持"""
        # 创建字符串类型的 DelayButFastSet
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # 测试添加字符串
        string_set.add("hello")
        string_set.add("world")
        
        # 测试包含检查
        self.assertTrue("hello" in string_set)
        self.assertTrue("world" in string_set)
        self.assertFalse("not_exist" in string_set)
        
        # 测试更新方法
        string_set.update("new", "items")
        self.assertTrue("new" in string_set)
        self.assertTrue("items" in string_set)

    def test_int_generic(self):
        """测试整数类型的泛型支持"""
        # 创建整数类型的 DelayButFastSet
        int_set: DelayButFastSet[int] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set2", 
            timeout=1
        )
        
        # 测试添加整数
        int_set.add(123)
        int_set.add(456)
        
        # 测试包含检查
        self.assertTrue(123 in int_set)
        self.assertTrue(456 in int_set)
        self.assertFalse(999 in int_set)
        
        # 测试更新方法
        int_set.update(789, 101)
        self.assertTrue(789 in int_set)
        self.assertTrue(101 in int_set)

    def test_float_generic(self):
        """测试浮点数类型的泛型支持"""
        # 创建浮点数类型的 DelayButFastSet
        float_set: DelayButFastSet[float] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # 测试添加浮点数
        float_set.add(3.14)
        float_set.add(2.718)
        
        # 测试包含检查
        self.assertTrue(3.14 in float_set)
        self.assertTrue(2.718 in float_set)
        self.assertFalse(1.0 in float_set)

    def test_str_method(self):
        """测试 __str__ 方法不影响功能"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # 添加少量数据
        string_set.add("a")
        string_set.add("b")
        string_set.add("c")
        
        # 测试 __str__ 方法
        str_result = str(string_set)
        self.assertIn("DelayButFastSet", str_result)
        self.assertIn("test_set:value", str_result)
        self.assertIn("test_set:version", str_result)
        
        # 确保 __str__ 调用后功能正常
        self.assertTrue("a" in string_set)
        self.assertTrue("b" in string_set)
        self.assertTrue("c" in string_set)

    def test_str_method_large_set(self):
        """测试大数据集时的 __str__ 方法"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # 添加大量数据
        for i in range(150):  # 超过100个元素
            string_set.add(f"item_{i}")
        
        # 测试 __str__ 方法
        str_result = str(string_set)
        self.assertIn("DelayButFastSet", str_result)
        self.assertIn("too many values", str_result)
        
        # 确保功能正常
        self.assertTrue("item_0" in string_set)
        self.assertTrue("item_149" in string_set)

    def test_discard_method(self):
        """测试 discard 方法"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        string_set.add("keep")
        string_set.add("remove")
        
        # 测试 discard
        string_set.discard("remove")
        self.assertTrue("keep" in string_set)
        self.assertFalse("remove" in string_set)
        
        # 测试不存在的元素
        string_set.discard("not_exist")  # 不应该报错

    def test_remove_method(self):
        """测试 remove 方法（discard 的别名）"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        string_set.add("keep")
        string_set.add("remove")
        
        # 测试 remove
        string_set.remove("remove")
        self.assertTrue("keep" in string_set)
        self.assertFalse("remove" in string_set)

    def test_len_method(self):
        """测试 __len__ 方法"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        self.assertEqual(len(string_set), 0)
        
        string_set.add("a")
        self.assertEqual(len(string_set), 1)
        
        string_set.add("b")
        self.assertEqual(len(string_set), 2)
        
        string_set.discard("a")
        self.assertEqual(len(string_set), 1)

    def test_iter_method(self):
        """测试 __iter__ 方法"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        string_set.add("a")
        string_set.add("b")
        string_set.add("c")
        
        # 测试迭代
        items = list(string_set)
        self.assertEqual(len(items), 3)
        self.assertIn("a", items)
        self.assertIn("b", items)
        self.assertIn("c", items)

    def test_subtraction_operation(self):
        """测试减法操作"""
        set1: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        set2: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set2", 
            timeout=1
        )
        
        set1.update("a", "b", "c")
        set2.update("b", "c", "d")
        
        # 测试 DelayButFastSet 之间的减法
        result = set1 - set2
        self.assertEqual(result, {"a"})
        
        # 测试与普通 set 的减法
        normal_set = {"b", "c", "d"}
        result = set1 - normal_set
        self.assertEqual(result, {"a"})

    def test_mixed_types_conversion(self):
        """测试混合类型的自动转换"""
        mixed_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # 测试不同类型自动转换为字符串
        mixed_set.add("string")
        mixed_set.add(123)
        mixed_set.add(3.14)
        
        # 验证所有值都被正确存储
        self.assertTrue("string" in mixed_set)
        self.assertTrue("123" in mixed_set)  # 整数被转换为字符串
        self.assertTrue("3.14" in mixed_set)  # 浮点数被转换为字符串

    def test_startup_init(self):
        """测试 startup_init 参数"""
        # 先创建一个集合并添加数据
        set1: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        set1.add("existing_data")
        
        # 创建另一个实例，使用 startup_init=True
        set2: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1,
            startup_init=True
        )
        
        # 验证数据被正确加载
        self.assertTrue("existing_data" in set2)


if __name__ == "__main__":
    unittest.main() 
