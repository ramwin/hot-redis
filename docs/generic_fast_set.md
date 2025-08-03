# DelayButFastSet 泛型支持

## 概述

`DelayButFastSet` 现在支持泛型，允许您指定集合中元素的类型，提供更好的类型安全性和 IDE 支持。

## 支持的泛型类型

- `DelayButFastSet[str]` - 字符串类型
- `DelayButFastSet[int]` - 整数类型  
- `DelayButFastSet[float]` - 浮点数类型

## 基本用法

### 字符串类型

```python
from hot_redis.fast_set import DelayButFastSet
from redis import Redis

# 创建字符串类型的集合
users: DelayButFastSet[str] = DelayButFastSet(
    redis_client=Redis(decode_responses=True),
    key="users",
    timeout=5
)

# 添加用户
users.add("alice")
users.add("bob")

# 检查用户是否存在
if "alice" in users:
    print("Alice 存在")

# 批量添加
users.update("charlie", "david", "eve")

# 移除用户
users.discard("bob")
```

### 整数类型

```python
# 创建整数类型的集合
user_ids: DelayButFastSet[int] = DelayButFastSet(
    redis_client=Redis(decode_responses=True),
    key="user_ids",
    timeout=5
)

# 添加用户ID
user_ids.add(1001)
user_ids.add(1002)

# 检查用户ID是否存在
if 1001 in user_ids:
    print("用户ID 1001 存在")
```

### 浮点数类型

```python
# 创建浮点数类型的集合
scores: DelayButFastSet[float] = DelayButFastSet(
    redis_client=Redis(decode_responses=True),
    key="scores",
    timeout=5
)

# 添加分数
scores.add(95.5)
scores.add(87.3)

# 检查分数是否存在
if 95.5 in scores:
    print("分数 95.5 存在")
```

## 类型转换

所有类型在内部都会被转换为字符串存储，但您可以使用原始类型进行操作：

```python
# 整数类型
user_ids: DelayButFastSet[int] = DelayButFastSet(redis_client, "user_ids")
user_ids.add(123)  # 内部存储为 "123"

# 检查时使用整数
if 123 in user_ids:  # 自动转换为字符串进行比较
    print("用户ID 123 存在")
```

## 集合操作

支持所有标准的集合操作：

```python
set1: DelayButFastSet[str] = DelayButFastSet(redis_client, "set1")
set2: DelayButFastSet[str] = DelayButFastSet(redis_client, "set2")

set1.update("a", "b", "c")
set2.update("b", "c", "d")

# 集合减法
difference = set1 - set2  # 结果: {"a"}

# 与普通集合的减法
normal_set = {"b", "c"}
difference = set1 - normal_set  # 结果: {"a"}
```

## 迭代和长度

```python
users: DelayButFastSet[str] = DelayButFastSet(redis_client, "users")
users.update("alice", "bob", "charlie")

# 获取长度
print(f"用户数量: {len(users)}")

# 迭代
for user in users:
    print(f"用户: {user}")

# 转换为列表
user_list = list(users)
```

## 字符串表示

`DelayButFastSet` 提供了有意义的字符串表示：

```python
users: DelayButFastSet[str] = DelayButFastSet(redis_client, "users")
users.add("alice")
users.add("bob")

print(str(users))
# 输出: DelayButFastSet:users:value:users:version: {'alice', 'bob'}

# 对于大数据集（超过100个元素）
for i in range(150):
    users.add(f"user_{i}")

print(str(users))
# 输出: DelayButFastSet:users:value:users:version: too many values...
```

## 性能特性

- **延迟加载**: 数据只在需要时从 Redis 加载
- **内存缓存**: 数据缓存在内存中，提供快速访问
- **版本控制**: 使用版本号确保数据一致性
- **自动刷新**: 在超时后自动刷新数据

## 类型安全

使用泛型可以获得更好的类型检查：

```python
# 类型安全的操作
users: DelayButFastSet[str] = DelayButFastSet(redis_client, "users")
users.add("alice")  # ✅ 正确
users.add(123)      # ✅ 自动转换为字符串
users.add(3.14)     # ✅ 自动转换为字符串

# IDE 会提供更好的自动完成和类型检查
```

## 注意事项

1. **内部存储**: 所有值在 Redis 中都存储为字符串
2. **类型转换**: 非字符串类型会自动转换为字符串
3. **向后兼容**: 现有代码无需修改即可使用
4. **性能**: 泛型支持不会影响性能

## 测试

运行测试以确保功能正常：

```bash
python -m pytest tests/test_fast_set.py -v
```

## 示例

查看完整的使用示例：

```bash
python examples/generic_fast_set_example.py
``` 
