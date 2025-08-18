#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Stress tests for DelayButFastDict

import time
import unittest
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

from hot_redis.fast_dict import DelayButFastDict
from redis import Redis


class TestFastDictStress(unittest.TestCase):
    """Stress tests and edge case tests"""

    def setUp(self):
        self.redis_client = Redis(decode_responses=True)
        # Clean using hash tags format
        self.redis_client.delete("{stress_dict}:value")
        self.redis_client.delete("{stress_dict}:version")

    def tearDown(self):
        # Clean using hash tags format
        self.redis_client.delete("{stress_dict}:value")
        self.redis_client.delete("{stress_dict}:version")

    def test_large_data_set(self):
        """Test large dataset performance"""
        test_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict",
            timeout=5
        )

        # Insert large amount of data
        start_time = time.time()
        for i in range(1000):
            test_dict[f"key_{i:04d}"] = f"value_{i:04d}"
        
        insert_time = time.time() - start_time
        print(f"Insert 1000 items time: {insert_time:.3f}s")

        # Read performance test
        start_time = time.time()
        for i in range(1000):
            _ = test_dict[f"key_{i:04d}"]
        
        read_time = time.time() - start_time
        print(f"Read 1000 items time: {read_time:.3f}s")

        # Verify data correctness
        self.assertEqual(len(test_dict), 1000)
        self.assertEqual(test_dict["key_0000"], "value_0000")
        self.assertEqual(test_dict["key_0999"], "value_0999")

    def test_frequent_refresh(self):
        """Test frequent refresh performance"""
        test_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict",
            timeout=0.1  # Very short timeout
        )

        # Add some initial data
        for i in range(100):
            test_dict[f"key_{i}"] = f"value_{i}"

        # Frequent access triggering refresh
        start_time = time.time()
        for i in range(500):
            if i % 50 == 0:
                time.sleep(0.11)  # Trigger timeout refresh
            _ = test_dict.get(f"key_{i % 100}")
        
        access_time = time.time() - start_time
        print(f"500 frequent accesses (with refresh) time: {access_time:.3f}s")

    def test_memory_usage_large_keys_values(self):
        """Test memory usage with large key-value pairs"""
        test_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict",
            timeout=10
        )

        # Create large key-value pairs
        large_key = "k" * 1000  # 1KB key
        large_value = "v" * 10000  # 10KB value

        test_dict[large_key] = large_value
        
        # Verify storage and retrieval
        self.assertEqual(test_dict[large_key], large_value)
        self.assertTrue(large_key in test_dict)

    def test_rapid_updates(self):
        """Test rapid updates to the same key"""
        test_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict",
            timeout=5
        )

        key = "rapid_update_key"
        
        # Rapidly update the same key
        for i in range(100):
            test_dict[key] = f"value_{i}"
            if i % 10 == 0:
                self.assertEqual(test_dict[key], f"value_{i}")

        # Verify final value
        self.assertEqual(test_dict[key], "value_99")

    def test_random_operations(self):
        """Test mixed random operations"""
        test_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict",
            timeout=2
        )

        operations = ['set', 'get', 'delete', 'update']
        keys = [f"key_{i}" for i in range(50)]
        
        # Execute random operations
        for _ in range(500):
            op = random.choice(operations)
            key = random.choice(keys)
            
            if op == 'set':
                value = f"value_{random.randint(1, 1000)}"
                test_dict[key] = value
            elif op == 'get':
                try:
                    _ = test_dict[key]
                except KeyError:
                    pass  # Normal case
            elif op == 'delete':
                try:
                    del test_dict[key]
                except KeyError:
                    pass  # Key might not exist
            elif op == 'update':
                update_data = {f"key_{i}": f"batch_value_{i}" for i in range(5)}
                test_dict.update(update_data)

        # After operations complete, verify dict is still usable
        test_dict["final_test"] = "final_value"
        self.assertEqual(test_dict["final_test"], "final_value")

    def test_version_race_condition(self):
        """Test version race conditions"""
        dict1 = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict",
            timeout=10  # Long timeout to avoid auto-refresh interference
        )
        
        dict2 = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict", 
            timeout=10
        )

        # Initialize data
        dict1["initial"] = "value"
        
        def worker1():
            for i in range(50):
                dict1[f"worker1_key_{i}"] = f"worker1_value_{i}"
                time.sleep(0.01)

        def worker2():
            for i in range(50):
                dict2[f"worker2_key_{i}"] = f"worker2_value_{i}"
                time.sleep(0.01)

        # Execute concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(worker1)
            future2 = executor.submit(worker2)
            
            future1.result()
            future2.result()

        # Refresh both instances and verify data consistency
        dict1.refresh()
        dict2.refresh()
        
        # Verify both instances see the same data
        self.assertEqual(len(dict1), len(dict2))
        
        for key in dict1.keys():
            self.assertTrue(key in dict2)
            self.assertEqual(dict1[key], dict2[key])

    def test_timeout_edge_cases(self):
        """Test timeout edge cases"""
        # Test very short timeout
        short_timeout_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict",
            timeout=0.01  # 10ms timeout
        )
        
        short_timeout_dict["test"] = "value"
        time.sleep(0.02)  # Wait for timeout
        self.assertEqual(short_timeout_dict["test"], "value")  # Should trigger refresh

        # Test long timeout
        self.redis_client.delete("{stress_dict}:value", "{stress_dict}:version")
        long_timeout_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict", 
            timeout=3600  # 1 hour timeout
        )
        
        long_timeout_dict["test"] = "value"
        
        # Ensure initial state
        self.assertFalse("external" in long_timeout_dict)
        
        # Create another instance to simulate external modification
        external_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict",
            timeout=1
        )
        external_dict["external"] = "external_value"
        
        # After forced refresh, should see external changes
        long_timeout_dict.refresh()
        self.assertTrue("external" in long_timeout_dict)

    def test_unicode_keys_values(self):
        """Test Unicode keys and values"""
        test_dict = DelayButFastDict[str, str](
            redis_client=self.redis_client,
            key="stress_dict",
            timeout=5
        )

        # Test various Unicode characters
        unicode_data = {
            "中文键": "中文值",
            "日本語": "こんにちは",
            "العربية": "مرحبا",
            "русский": "Привет",
            "emoji_😀": "emoji_😀_value",
            "数字_123": "值_456"
        }

        # Insert Unicode data
        for key, value in unicode_data.items():
            test_dict[key] = value

        # Verify Unicode data
        for key, value in unicode_data.items():
            self.assertTrue(key in test_dict)
            self.assertEqual(test_dict[key], value)

        # Test Unicode operations
        self.assertEqual(len(test_dict), len(unicode_data))
        
        # Test iteration over Unicode keys
        keys = list(test_dict.keys())
        for key in unicode_data.keys():
            self.assertIn(key, keys)


if __name__ == "__main__":
    unittest.main(verbosity=2)
