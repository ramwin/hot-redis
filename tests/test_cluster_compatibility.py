#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Redis Cluster compatibility tests

import unittest
from unittest.mock import patch, MagicMock

from hot_redis.fast_dict import DelayButFastDict
from hot_redis.fast_set import DelayButFastSet
from redis import Redis


class TestClusterCompatibility(unittest.TestCase):
    """Test Redis Cluster compatibility"""

    def setUp(self):
        self.redis_client = Redis(decode_responses=True)
        # Clean test data
        keys_to_delete = [
            "{test_key}:value", "{test_key}:version",
            "{cluster_dict}:value", "{cluster_dict}:version",
            "{cluster_set}:value", "{cluster_set}:version"
        ]
        for key in keys_to_delete:
            self.redis_client.delete(key)

    def tearDown(self):
        # Clean test data
        keys_to_delete = [
            "{test_key}:value", "{test_key}:version",
            "{cluster_dict}:value", "{cluster_dict}:version", 
            "{cluster_set}:value", "{cluster_set}:version"
        ]
        for key in keys_to_delete:
            self.redis_client.delete(key)

    def test_hash_tags_in_keys(self):
        """Test keys use hash tags format"""
        # Test DelayButFastDict
        fast_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="test_key",
            timeout=5
        )
        
        # Verify key format
        self.assertEqual(fast_dict.value_key, "{test_key}:value")
        self.assertEqual(fast_dict.version_key, "{test_key}:version")
        
        # Test DelayButFastSet
        fast_set = DelayButFastSet[str](
            redis_client=self.redis_client,
            key="test_key", 
            timeout=5
        )
        
        # Verify key format
        self.assertEqual(fast_set.value_key, "{test_key}:value")
        self.assertEqual(fast_set.version_key, "{test_key}:version")

    def test_same_slot_operations_dict(self):
        """Test DelayButFastDict operations in same slot"""
        fast_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="cluster_dict",
            timeout=5
        )
        
        # Basic operations should work normally
        fast_dict["key1"] = "value1"
        fast_dict["key2"] = "value2"
        
        # Verify data is stored correctly
        self.assertEqual(fast_dict["key1"], "value1")
        self.assertEqual(fast_dict["key2"], "value2")
        self.assertEqual(len(fast_dict), 2)
        
        # Test delete operations
        del fast_dict["key1"]
        self.assertFalse("key1" in fast_dict)
        self.assertTrue("key2" in fast_dict)

    def test_same_slot_operations_set(self):
        """Test DelayButFastSet operations in same slot"""
        fast_set = DelayButFastSet[str](
            redis_client=self.redis_client,
            key="cluster_set",
            timeout=5
        )
        
        # Basic operations should work normally
        fast_set.add("item1")
        fast_set.add("item2")
        
        # Verify data is stored correctly
        self.assertTrue("item1" in fast_set)
        self.assertTrue("item2" in fast_set)
        self.assertEqual(len(fast_set), 2)
        
        # Test delete operations
        fast_set.discard("item1")
        self.assertFalse("item1" in fast_set)
        self.assertTrue("item2" in fast_set)

    def test_pipeline_operations_dict(self):
        """Test DelayButFastDict pipeline operations"""
        fast_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="cluster_dict",
            timeout=5
        )
        
        # These operations internally use pipeline, should not error
        fast_dict["test_key"] = "test_value"
        
        # Verify operation succeeded
        self.assertEqual(fast_dict["test_key"], "test_value")
        
        # Test update operations (also uses pipeline)
        fast_dict.update({"key1": "value1", "key2": "value2"})
        self.assertEqual(fast_dict["key1"], "value1")
        self.assertEqual(fast_dict["key2"], "value2")

    def test_pipeline_operations_set(self):
        """Test DelayButFastSet pipeline operations"""
        fast_set = DelayButFastSet[str](
            redis_client=self.redis_client,
            key="cluster_set", 
            timeout=5
        )
        
        # These operations internally use pipeline, should not error
        fast_set.add("test_item")
        
        # Verify operation succeeded
        self.assertTrue("test_item" in fast_set)
        
        # Test batch update operations
        fast_set.update("item1", "item2", "item3")
        self.assertTrue("item1" in fast_set)
        self.assertTrue("item2" in fast_set)
        self.assertTrue("item3" in fast_set)

    def test_version_synchronization_dict(self):
        """Test DelayButFastDict version synchronization in cluster"""
        dict1 = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="cluster_dict",
            timeout=10
        )
        
        dict2 = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="cluster_dict",
            timeout=10
        )
        
        # dict1 adds data
        dict1["sync_test"] = "value1"
        
        # dict2 should be able to see the data through version sync
        dict2.refresh()
        self.assertTrue("sync_test" in dict2)
        self.assertEqual(dict2["sync_test"], "value1")

    def test_version_synchronization_set(self):
        """Test DelayButFastSet version synchronization in cluster"""
        set1 = DelayButFastSet[str](
            redis_client=self.redis_client,
            key="cluster_set",
            timeout=10
        )
        
        set2 = DelayButFastSet[str](
            redis_client=self.redis_client,
            key="cluster_set", 
            timeout=10
        )
        
        # set1 adds data
        set1.add("sync_test")
        
        # set2 should be able to see the data through version sync
        set2.refresh()
        self.assertTrue("sync_test" in set2)

    def test_hash_tag_extraction(self):
        """Test hash tag correctness"""
        # Verify different keys produce same hash tag part
        test_cases = [
            ("user_123", "{user_123}:value", "{user_123}:version"),
            ("cache_data", "{cache_data}:value", "{cache_data}:version"),
            ("session_abc", "{session_abc}:value", "{session_abc}:version"),
        ]
        
        for base_key, expected_value_key, expected_version_key in test_cases:
            fast_dict = DelayButFastDict[str, str](
                redis_client=self.redis_client,
                key=base_key,
                timeout=5
            )
            
            self.assertEqual(fast_dict.value_key, expected_value_key)
            self.assertEqual(fast_dict.version_key, expected_version_key)
            
            # Clean up
            self.redis_client.delete(expected_value_key, expected_version_key)

    def test_clear_operations_with_hash_tags(self):
        """Test clear operations with hash tags"""
        fast_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="cluster_dict",
            timeout=5
        )
        
        # Add data
        fast_dict["key1"] = "value1"
        fast_dict["key2"] = "value2"
        self.assertEqual(len(fast_dict), 2)
        
        # Clear operation (uses pipeline)
        fast_dict.clear()
        self.assertEqual(len(fast_dict), 0)
        
        # Verify Redis data is also cleared
        self.assertEqual(self.redis_client.hlen("{cluster_dict}:value"), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)