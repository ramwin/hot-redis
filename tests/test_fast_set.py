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
        """Clean Redis data before each test"""
        self.redis_client = Redis(decode_responses=True)
        # Clean using hash tags format
        self.redis_client.delete("{test_set}:value")
        self.redis_client.delete("{test_set}:version")
        self.redis_client.delete("{test_set2}:value")
        self.redis_client.delete("{test_set2}:version")

    def test_string_generic(self):
        """Test string type generic support"""
        # Create string type DelayButFastSet
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # Test adding strings
        string_set.add("hello")
        string_set.add("world")
        
        # Test contains check
        self.assertTrue("hello" in string_set)
        self.assertTrue("world" in string_set)
        self.assertFalse("not_exist" in string_set)
        
        # Test update method
        string_set.update("new", "items")
        self.assertTrue("new" in string_set)
        self.assertTrue("items" in string_set)

    def test_int_generic(self):
        """Test integer type generic support"""
        # Create integer type DelayButFastSet
        int_set: DelayButFastSet[int] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set2", 
            timeout=1
        )
        
        # Test adding integers
        int_set.add(123)
        int_set.add(456)
        
        # Test contains check
        self.assertTrue(123 in int_set)
        self.assertTrue(456 in int_set)
        self.assertFalse(999 in int_set)
        
        # Test update method
        int_set.update(789, 101)
        self.assertTrue(789 in int_set)
        self.assertTrue(101 in int_set)

    def test_float_generic(self):
        """Test float type generic support"""
        # Create float type DelayButFastSet
        float_set: DelayButFastSet[float] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # Test adding floats
        float_set.add(3.14)
        float_set.add(2.718)
        
        # Test contains check
        self.assertTrue(3.14 in float_set)
        self.assertTrue(2.718 in float_set)
        self.assertFalse(1.0 in float_set)

    def test_str_method(self):
        """Test __str__ method doesn't affect functionality"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # Add some data
        string_set.add("a")
        string_set.add("b")
        string_set.add("c")
        
        # Test __str__ method
        str_result = str(string_set)
        self.assertIn("DelayButFastSet", str_result)
        self.assertIn("{test_set}:value", str_result)
        self.assertIn("{test_set}:version", str_result)
        
        # Ensure functionality works after __str__ call
        self.assertTrue("a" in string_set)
        self.assertTrue("b" in string_set)
        self.assertTrue("c" in string_set)

    def test_str_method_large_set(self):
        """Test __str__ method with large dataset"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # Add large amount of data
        for i in range(150):  # More than 100 elements
            string_set.add(f"item_{i}")
        
        # Test __str__ method
        str_result = str(string_set)
        self.assertIn("DelayButFastSet", str_result)
        self.assertIn("too many values", str_result)
        
        # Ensure functionality works
        self.assertTrue("item_0" in string_set)
        self.assertTrue("item_149" in string_set)

    def test_discard_method(self):
        """Test discard method"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        string_set.add("keep")
        string_set.add("remove")
        
        # Test discard
        string_set.discard("remove")
        self.assertTrue("keep" in string_set)
        self.assertFalse("remove" in string_set)
        
        # Test discard non-existent element
        string_set.discard("not_exist")  # Should not error

    def test_remove_method(self):
        """Test remove method (alias for discard)"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        string_set.add("keep")
        string_set.add("remove")
        
        # Test remove
        string_set.remove("remove")
        self.assertTrue("keep" in string_set)
        self.assertFalse("remove" in string_set)

    def test_len_method(self):
        """Test __len__ method"""
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
        """Test __iter__ method"""
        string_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        string_set.add("a")
        string_set.add("b")
        string_set.add("c")
        
        # Test iteration
        items = list(string_set)
        self.assertEqual(len(items), 3)
        self.assertIn("a", items)
        self.assertIn("b", items)
        self.assertIn("c", items)

    def test_subtraction_operation(self):
        """Test subtraction operations"""
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
        
        # Test subtraction between DelayButFastSet instances
        result = set1 - set2
        self.assertEqual(result, {"a"})
        
        # Test subtraction with regular set
        normal_set = {"b", "c", "d"}
        result = set1 - normal_set
        self.assertEqual(result, {"a"})

    def test_mixed_types_conversion(self):
        """Test automatic conversion of mixed types"""
        mixed_set: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        
        # Test different types auto-converted to strings
        mixed_set.add("string")
        mixed_set.add(123)
        mixed_set.add(3.14)
        
        # Verify all values are stored correctly
        self.assertTrue("string" in mixed_set)
        self.assertTrue("123" in mixed_set)  # Integer converted to string
        self.assertTrue("3.14" in mixed_set)  # Float converted to string

    def test_startup_init(self):
        """Test startup_init parameter"""
        # First create a set and add data
        set1: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1
        )
        set1.add("existing_data")
        
        # Create another instance using startup_init=True
        set2: DelayButFastSet[str] = DelayButFastSet(
            redis_client=self.redis_client, 
            key="test_set", 
            timeout=1,
            startup_init=True
        )
        
        # Verify data is loaded correctly
        self.assertTrue("existing_data" in set2)


if __name__ == "__main__":
    unittest.main()