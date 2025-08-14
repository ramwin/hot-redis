# Redis Cluster 设置指南

## 问题分析
您的 Redis 实例在集群模式下运行，但集群配置不完整，导致以下问题：
1. MOVED 错误：键被重定向到不存在的节点
2. CROSSSLOT 错误：多个键不在同一个哈希槽中
3. 连接超时：blmpop 命令等待时超时

## 解决方案

### 短期解决方案（推荐用于开发）
使用单个 Redis 实例进行开发和测试：

```python
from redis import Redis
from hot_redis.multi_wait_task import MultiWaitTask

# 使用默认 Redis 连接
client = Redis(
    host="localhost",
    port=6379,
    decode_responses=True,
    socket_timeout=30,  # 增加超时时间
    socket_connect_timeout=10
)

# 创建任务
task = MultiWaitTask(
    client=client,
    keys=["list1", "list2"],
    batch_size=3
)

# 添加任务
task.add_task("list1", {"task": "task1", "data": "data1"})

# 获取任务
result = task.get_tasks()
if result:
    key, tasks = result
    print(f"键: {key}")
    print(f"任务: {tasks}")
```

### 长期解决方案
正确配置 Redis Cluster：

1. 创建配置文件 redis-7000.conf：
   ```
   port 7000
   cluster-enabled yes
   cluster-config-file nodes-7000.conf
   cluster-node-timeout 5000
   appendonly yes
   ```

2. 为所有节点创建配置文件（7001, 7002, 7003, 7004, 7005）

3. 启动所有节点：
   ```bash
   redis-server redis-7000.conf
   redis-server redis-7001.conf
   redis-server redis-7002.conf
   redis-server redis-7003.conf
   redis-server redis-7004.conf
   redis-server redis-7005.conf
   ```

4. 创建集群：
   ```bash
   redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 --cluster-replicas 0
   ```

5. 验证集群状态：
   ```bash
   redis-cli -p 7000 cluster info
   redis-cli -p 7000 cluster nodes
   ```

## 测试
运行修复后的测试：
```bash
python tests/test_multi_wait_task_final.py
```
