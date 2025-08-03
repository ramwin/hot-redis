#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DelayButFastSet 泛型功能使用示例
"""

from redis import Redis
from hot_redis.fast_set import DelayButFastSet


def example_string_generic():
    """字符串类型泛型示例"""
    print("=== 字符串类型泛型示例 ===")
    
    redis_client = Redis(decode_responses=True)
    
    # 创建字符串类型的 DelayButFastSet
    users: DelayButFastSet[str] = DelayButFastSet(
        redis_client=redis_client,
        key="users",
        timeout=5
    )
    
    # 添加用户
    users.add("alice")
    users.add("bob")
    users.add("charlie")
    
    # 检查用户是否存在
    print(f"'alice' in users: {'alice' in users}")
    print(f"'david' in users: {'david' in users}")
    
    # 批量添加用户
    users.update("david", "eve", "frank")
    
    # 移除用户
    users.discard("bob")
    
    # 打印集合信息
    print(f"用户数量: {len(users)}")
    print(f"所有用户: {list(users)}")
    print(f"集合字符串表示: {users}")


def example_int_generic():
    """整数类型泛型示例"""
    print("\n=== 整数类型泛型示例 ===")
    
    redis_client = Redis(decode_responses=True)
    
    # 创建整数类型的 DelayButFastSet
    user_ids: DelayButFastSet[int] = DelayButFastSet(
        redis_client=redis_client,
        key="user_ids",
        timeout=5
    )
    
    # 添加用户ID
    user_ids.add(1001)
    user_ids.add(1002)
    user_ids.add(1003)
    
    # 检查用户ID是否存在
    print(f"1001 in user_ids: {1001 in user_ids}")
    print(f"9999 in user_ids: {9999 in user_ids}")
    
    # 批量添加用户ID
    user_ids.update(1004, 1005, 1006)
    
    # 移除用户ID
    user_ids.remove(1002)
    
    # 打印集合信息
    print(f"用户ID数量: {len(user_ids)}")
    print(f"所有用户ID: {list(user_ids)}")


def example_float_generic():
    """浮点数类型泛型示例"""
    print("\n=== 浮点数类型泛型示例 ===")
    
    redis_client = Redis(decode_responses=True)
    
    # 创建浮点数类型的 DelayButFastSet
    scores: DelayButFastSet[float] = DelayButFastSet(
        redis_client=redis_client,
        key="scores",
        timeout=5
    )
    
    # 添加分数
    scores.add(95.5)
    scores.add(87.3)
    scores.add(92.1)
    
    # 检查分数是否存在
    print(f"95.5 in scores: {95.5 in scores}")
    print(f"80.0 in scores: {80.0 in scores}")
    
    # 批量添加分数
    scores.update(88.9, 91.2, 94.7)
    
    # 移除分数
    scores.discard(87.3)
    
    # 打印集合信息
    print(f"分数数量: {len(scores)}")
    print(f"所有分数: {list(scores)}")


def example_mixed_types():
    """混合类型示例（自动转换为字符串）"""
    print("\n=== 混合类型示例 ===")
    
    redis_client = Redis(decode_responses=True)
    
    # 创建字符串类型的 DelayButFastSet，但可以接受多种类型
    mixed_set: DelayButFastSet[str] = DelayButFastSet(
        redis_client=redis_client,
        key="mixed_data",
        timeout=5
    )
    
    # 添加不同类型的数据
    mixed_set.add("字符串")
    mixed_set.add(123)  # 整数
    mixed_set.add(3.14)  # 浮点数
    
    # 检查数据是否存在
    print(f"'字符串' in mixed_set: {'字符串' in mixed_set}")
    print(f"'123' in mixed_set: {'123' in mixed_set}")  # 注意：整数被转换为字符串
    print(f"'3.14' in mixed_set: {'3.14' in mixed_set}")  # 注意：浮点数被转换为字符串
    
    # 打印集合信息
    print(f"混合数据数量: {len(mixed_set)}")
    print(f"所有数据: {list(mixed_set)}")


def example_set_operations():
    """集合操作示例"""
    print("\n=== 集合操作示例 ===")
    
    redis_client = Redis(decode_responses=True)
    
    # 创建两个集合
    set1: DelayButFastSet[str] = DelayButFastSet(
        redis_client=redis_client,
        key="set1",
        timeout=5
    )
    
    set2: DelayButFastSet[str] = DelayButFastSet(
        redis_client=redis_client,
        key="set2",
        timeout=5
    )
    
    # 添加数据
    set1.update("a", "b", "c", "d")
    set2.update("b", "c", "d", "e")
    
    # 集合减法操作
    difference = set1 - set2
    print(f"set1 - set2 = {difference}")
    
    # 与普通集合的减法操作
    normal_set = {"b", "c", "d"}
    difference = set1 - normal_set
    print(f"set1 - normal_set = {difference}")


if __name__ == "__main__":
    # 运行所有示例
    example_string_generic()
    example_int_generic()
    example_float_generic()
    example_mixed_types()
    example_set_operations()
    
    print("\n=== 所有示例运行完成 ===") 
