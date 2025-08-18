#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


import time
import unittest
import threading
import logging
from typing import Dict
from concurrent.futures import ThreadPoolExecutor

from hot_redis.fast_dict import DelayButFastDict
from redis import Redis

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
LOGGER = logging.getLogger(__name__)


class TestFastDictGeneric(unittest.TestCase):

    def setUp(self):
        """Clean Redis data before each test"""
        self.redis_client = Redis(decode_responses=True)
        # Clean using hash tags format
        self.redis_client.delete("{test_dict}:value")
        self.redis_client.delete("{test_dict}:version")
        self.redis_client.delete("{test_dict2}:value")
        self.redis_client.delete("{test_dict2}:version")

    def tearDown(self):
        """Clean Redis data after each test"""
        # Clean using hash tags format
        self.redis_client.delete("{test_dict}:value")
        self.redis_client.delete("{test_dict}:version")
        self.redis_client.delete("{test_dict2}:value")
        self.redis_client.delete("{test_dict2}:version")

    def test_string_generic(self):
        """Test string type generic support"""
        string_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        # 测试基本操作
        string_dict["hello"] = "world"
        string_dict["foo"] = "bar"
        
        # 测试获取
        self.assertEqual(string_dict["hello"], "world")
        self.assertEqual(string_dict["foo"], "bar")
        
        # 测试包含检查
        self.assertTrue("hello" in string_dict)
        self.assertTrue("foo" in string_dict)
        self.assertFalse("not_exist" in string_dict)

    def test_int_keys_string_values(self):
        """Test integer keys with string values"""
        int_str_dict: DelayButFastDict[int, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict2", 
            timeout=1
        )
        
        # 测试整数键
        int_str_dict[123] = "value_123"
        int_str_dict[456] = "value_456"
        
        # 测试获取
        self.assertEqual(int_str_dict[123], "value_123")
        self.assertEqual(int_str_dict[456], "value_456")
        
        # 测试包含检查
        self.assertTrue(123 in int_str_dict)
        self.assertTrue(456 in int_str_dict)
        self.assertFalse(999 in int_str_dict)

    def test_mixed_types(self):
        """Test automatic conversion of mixed types"""
        mixed_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        # 测试不同类型自动转换为字符串
        mixed_dict["int_key"] = "123"
        mixed_dict["float_key"] = "3.14"
        mixed_dict[123] = "int_as_key"  # 整数键转为字符串
        
        # 验证所有值都被正确存储和检索
        self.assertEqual(mixed_dict["int_key"], "123")
        self.assertEqual(mixed_dict["float_key"], "3.14")
        self.assertEqual(mixed_dict["123"], "int_as_key")  # 整数键被转换为字符串

    def test_get_method(self):
        """Test get method"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        test_dict["exist"] = "value"
        
        # 测试存在的键
        self.assertEqual(test_dict.get("exist"), "value")
        
        # 测试不存在的键
        self.assertIsNone(test_dict.get("not_exist"))
        self.assertEqual(test_dict.get("not_exist", "default"), "default")

    def test_setdefault_method(self):
        """Test setdefault method"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        # 测试不存在的键
        result = test_dict.setdefault("new_key", "default_value")
        self.assertEqual(result, "default_value")
        self.assertEqual(test_dict["new_key"], "default_value")
        
        # 测试已存在的键
        result = test_dict.setdefault("new_key", "another_value")
        self.assertEqual(result, "default_value")  # 应该返回原值
        self.assertEqual(test_dict["new_key"], "default_value")  # 值不应该改变

    def test_pop_method(self):
        """Test pop method"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        test_dict["key1"] = "value1"
        test_dict["key2"] = "value2"
        
        # 测试弹出存在的键
        result = test_dict.pop("key1")
        self.assertEqual(result, "value1")
        self.assertFalse("key1" in test_dict)
        self.assertTrue("key2" in test_dict)
        
        # 测试弹出不存在的键（有默认值）
        result = test_dict.pop("not_exist", "default")
        self.assertEqual(result, "default")
        
        # 测试弹出不存在的键（无默认值，应该抛出异常）
        with self.assertRaises(KeyError):
            test_dict.pop("not_exist")

    def test_update_method(self):
        """Test update method"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        # 测试使用字典更新
        test_dict.update({"key1": "value1", "key2": "value2"})
        self.assertEqual(test_dict["key1"], "value1")
        self.assertEqual(test_dict["key2"], "value2")
        
        # 测试使用关键字参数更新
        test_dict.update(key3="value3", key4="value4")
        self.assertEqual(test_dict["key3"], "value3")
        self.assertEqual(test_dict["key4"], "value4")
        
        # 测试使用可迭代对象更新
        test_dict.update([("key5", "value5"), ("key6", "value6")])
        self.assertEqual(test_dict["key5"], "value5")
        self.assertEqual(test_dict["key6"], "value6")
        
        # 测试传递太多参数的错误情况
        with self.assertRaises(TypeError):
            test_dict.update({"key": "value"}, {"key2": "value2"})

    def test_keys_values_items(self):
        """Test keys, values, items methods"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        test_dict["key1"] = "value1"
        test_dict["key2"] = "value2"
        test_dict["key3"] = "value3"
        
        # 测试 keys
        keys = list(test_dict.keys())
        self.assertEqual(len(keys), 3)
        self.assertIn("key1", keys)
        self.assertIn("key2", keys)
        self.assertIn("key3", keys)
        
        # 测试 values
        values = list(test_dict.values())
        self.assertEqual(len(values), 3)
        self.assertIn("value1", values)
        self.assertIn("value2", values)
        self.assertIn("value3", values)
        
        # 测试 items
        items = list(test_dict.items())
        self.assertEqual(len(items), 3)
        self.assertIn(("key1", "value1"), items)
        self.assertIn(("key2", "value2"), items)
        self.assertIn(("key3", "value3"), items)

    def test_clear_method(self):
        """Test clear method"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        test_dict["key1"] = "value1"
        test_dict["key2"] = "value2"
        self.assertEqual(len(test_dict), 2)
        
        # 测试清空
        test_dict.clear()
        self.assertEqual(len(test_dict), 0)
        self.assertFalse("key1" in test_dict)
        self.assertFalse("key2" in test_dict)

    def test_len_method(self):
        """Test __len__ method"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        self.assertEqual(len(test_dict), 0)
        
        test_dict["a"] = "1"
        self.assertEqual(len(test_dict), 1)
        
        test_dict["b"] = "2"
        self.assertEqual(len(test_dict), 2)
        
        del test_dict["a"]
        self.assertEqual(len(test_dict), 1)

    def test_iter_method(self):
        """Test __iter__ method"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        test_dict["a"] = "1"
        test_dict["b"] = "2"
        test_dict["c"] = "3"
        
        # 测试迭代键
        keys = list(test_dict)
        self.assertEqual(len(keys), 3)
        self.assertIn("a", keys)
        self.assertIn("b", keys)
        self.assertIn("c", keys)

    def test_delete_method(self):
        """Test __delitem__ method"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        test_dict["keep"] = "keep_value"
        test_dict["delete"] = "delete_value"
        
        # 测试删除存在的键
        del test_dict["delete"]
        self.assertTrue("keep" in test_dict)
        self.assertFalse("delete" in test_dict)
        
        # 删除不存在的键不应该报错（只是不执行任何操作）
        del test_dict["not_exist"]  # 不应该抛出异常

    def test_key_error_handling(self):
        """Test KeyError handling"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        # 测试获取不存在的键
        with self.assertRaises(KeyError):
            _ = test_dict["not_exist"]

    def test_str_repr_methods(self):
        """Test __str__ and __repr__ methods"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        # 测试少量数据
        test_dict["a"] = "1"
        test_dict["b"] = "2"
        test_dict["c"] = "3"
        
        str_result = str(test_dict)
        repr_result = repr(test_dict)
        
        self.assertIn("DelayButFastDict", str_result)
        self.assertIn("{test_dict}:value", str_result)
        self.assertIn("{test_dict}:version", str_result)
        
        self.assertIn("DelayButFastDict", repr_result)
        self.assertIn("{test_dict}:value", repr_result)
        self.assertIn("{test_dict}:version", repr_result)

    def test_str_repr_methods_large_dict(self):
        """Test __str__ and __repr__ methods with large dataset"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        # 添加大量数据
        for i in range(150):  # 超过100个元素
            test_dict[f"key_{i}"] = f"value_{i}"
        
        str_result = str(test_dict)
        repr_result = repr(test_dict)
        
        self.assertIn("DelayButFastDict", str_result)
        self.assertIn("too many values", str_result)
        
        self.assertIn("DelayButFastDict", repr_result)
        self.assertIn("too many values", repr_result)

    def test_persistence_across_instances(self):
        """Test persistence across instances"""
        # 第一个实例写入数据
        dict1: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        dict1["persistent"] = "data"
        
        # 第二个实例读取数据
        dict2: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1,
            startup_init=True
        )
        
        # 验证数据持久化
        self.assertTrue("persistent" in dict2)
        self.assertEqual(dict2["persistent"], "data")

    def test_version_invalidation(self):
        """Test version invalidation mechanism"""
        dict1: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=10  # 长超时时间
        )
        
        dict2: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=10
        )
        
        dict1["test"] = "value1"
        
        # dict2 修改数据，这会更新版本
        dict2["test2"] = "value2"
        
        # 强制刷新 dict1
        dict1.refresh()
        
        # dict1 应该看到 dict2 的更改
        self.assertTrue("test2" in dict1)
        self.assertEqual(dict1["test2"], "value2")

    def test_startup_init(self):
        """Test startup_init parameter"""
        # 先创建一个字典并添加数据
        dict1: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        dict1["existing"] = "data"
        
        # 创建另一个实例，使用 startup_init=True
        dict2: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1,
            startup_init=True
        )
        
        # 验证数据被正确加载
        self.assertTrue("existing" in dict2)
        self.assertEqual(dict2["existing"], "data")

    def test_timeout_refresh(self):
        """Test automatic timeout refresh"""
        dict1: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1  # 1秒超时
        )
        
        dict1["test"] = "initial"
        
        # 创建另一个实例直接修改Redis
        dict2: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        dict2["test"] = "modified"
        
        # 等待超时
        time.sleep(1.1)
        
        # 访问应该触发刷新
        self.assertEqual(dict1["test"], "modified")

    def test_empty_key_error(self):
        """Test empty key error handling"""
        with self.assertRaises(ValueError):
            DelayButFastDict(
                redis_client=self.redis_client, 
                key="", 
                timeout=1
            )

    def test_concurrent_access(self):
        """Test concurrent access"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        def worker(thread_id):
            for i in range(10):
                key = f"thread_{thread_id}_key_{i}"
                value = f"thread_{thread_id}_value_{i}"
                test_dict[key] = value
                
                # Wait briefly to ensure data synchronization
                time.sleep(0.01)
                
                # Force refresh to ensure data consistency
                test_dict.refresh()
                
                # Verify write operation
                if key in test_dict:
                    self.assertEqual(test_dict[key], value)
        
        # Start multiple concurrent threads
        with ThreadPoolExecutor(max_workers=3) as executor:  # Reduce concurrency level
            futures = [executor.submit(worker, i) for i in range(3)]
            for future in futures:
                future.result()  # Wait for completion and check exceptions
        
        # Force refresh and verify final result
        test_dict.refresh()
        self.assertEqual(len(test_dict), 30)  # 3 threads × 10 keys

    def test_memory_consistency(self):
        """Test memory consistency"""
        test_dict: DelayButFastDict[str, str] = DelayButFastDict(
            redis_client=self.redis_client, 
            key="test_dict", 
            timeout=1
        )
        
        # Add data
        test_dict["key1"] = "value1"
        test_dict["key2"] = "value2"
        
        # Directly modify Redis data to simulate external changes
        self.redis_client.hset("{test_dict}:value", "key3", "value3")
        self.redis_client.incr("{test_dict}:version")
        
        # Force refresh
        test_dict.refresh()
        
        # Verify local cache is synchronized with Redis
        self.assertTrue("key3" in test_dict)
        self.assertEqual(test_dict["key3"], "value3")


if __name__ == "__main__":
    unittest.main()
