# 快速开始指南

## 🚀 5分钟快速上手

### 1. 环境准备

```bash
# 安装依赖
pip install -r requirements.txt

# 启动 Redis (选择一个方式)
# macOS
brew services start redis

# Ubuntu
sudo systemctl start redis

# Docker
docker run -p 6379:6379 redis:latest
```

### 2. 创建测试数据

```python
from data.redis_connector import RedisConnector

# 连接 Redis
connector = RedisConnector()

# 存储测试数据
connector.store_json_variable("test", "user_info", {
    "name": "张三",
    "age": 30,
    "city": "北京"
})

connector.store_direct_variable("test", "store_count", "100")
```

### 3. 基本使用

```python
from data.redis_connector import RedisConnector
from data.dataloader import DataLoader
from core.engine import RuleEngine
from core.nodes import LogicNode

# 1. 初始化
connector = RedisConnector()
loader = DataLoader(connector)
engine = RuleEngine("demo")

# 2. 定义数据配置
data_config = {
    "namespace": "test",
    "placeholder": [
        {"name": "store_nbr", "description": "门店号"}
    ],
    "variable": [
        {
            "name": "user_info",
            "redis_config": {
                "prefix": [],
                "field": "user_info",
                "type": "json"
            }
        },
        {
            "name": "store_count",
            "redis_config": {
                "prefix": [],
                "field": "store_count",
                "type": "value"
            }
        }
    ]
}

# 3. 加载数据
placeholders = {"store_nbr": "123"}
loaded_data = loader.load_data(data_config, placeholders, "2025-01-01 00:00:00")

# 4. 创建规则
logic_node = LogicNode("process")
logic_node.set_logic("""
# 处理数据
user_name = user_info.get('name', '未知')
user_age = user_info.get('age', 0)
total_stores = int(store_count)

# 计算结果
result = f"{user_name}管理{total_stores}家门店，年龄{user_age}岁"
""")
logic_node.set_tracked_variables(["result"])

# 5. 执行引擎
engine.add_dependency(None, logic_node)
results = engine.execute("2025-01-01 00:00:00", placeholders, loaded_data)

# 6. 查看结果
print("执行结果:")
for node_name, result in results.items():
    if result.success:
        print(f"✅ {node_name}: {result.data}")
    else:
        print(f"❌ {node_name}: {result.error}")
```

## 📊 数据类型示例

### Value 类型
```python
# 存储
connector.store_direct_variable("test", "count", "100")

# 配置
{
    "name": "count",
    "redis_config": {
        "field": "count",
        "type": "value"
    }
}
```

### JSON 类型
```python
# 存储
connector.store_json_variable("test", "config", {
    "enabled": True,
    "threshold": 0.8
})

# 配置
{
    "name": "config",
    "redis_config": {
        "field": "config",
        "type": "json"
    }
}
```

### Timeseries 类型
```python
# 存储
import time
timestamp = time.time()
connector.add_timeseries_point("test", "sales", timestamp, {
    "amount": 1000,
    "quantity": 10
})

# 配置
{
    "name": "sales_data",
    "redis_config": {
        "field": "sales",
        "type": "timeseries",
        "from_datetime": "${yyyy-MM-dd-7d}",
        "to_datetime": "${yyyy-MM-dd}",
        "drop_duplicate": "keep_latest"
    }
}
```

### Densets 类型
```python
# 存储 - 使用schema保证字段顺序
schema = "date:string,qty:int,price:double"
connector.add_densets_point("test", "forecast", timestamp, {
    "price": 10.5,  # 字段顺序不重要，schema会保证顺序
    "date": "2025-01-01",
    "qty": 100
}, schema=schema)

# 配置
{
    "name": "forecast_data",
    "redis_config": {
        "field": "forecast",
        "type": "densets",
        "split": ",",
        "schema": "date:string,qty:int,price:double"
    }
}
```

## 🔧 节点类型示例

### LogicNode - 逻辑处理
```python
logic_node = LogicNode("calculate")
logic_node.set_logic("""
# 计算平均值
avg_value = sum(values) / len(values)
result = {"average": avg_value, "count": len(values)}
""")
logic_node.set_tracked_variables(["result"])
```

### GateNode - 条件判断
```python
gate_node = GateNode("check_threshold")
gate_node.set_condition("value > 100")
```

### CollectionNode - 数据收集
```python
collection_node = CollectionNode("merge_data")
collection_node.set_logic("""
import pandas as pd
# 合并所有上游数据
df_list = []
for node_name in collection:
    if 'data' in collection[node_name]:
        df_list.append(collection[node_name]['data'])
        
if df_list:
    output = pd.concat(df_list, ignore_index=True)
else:
    output = pd.DataFrame()
""")
collection_node.set_tracked_variables(["output"])
```

## 📋 Schema 验证示例

### 节点输入验证
```python
logic_node.add_expected_input_schema("user_count", "int")
logic_node.add_expected_input_schema("sales_data", "ds:string,amount:double,quantity:int")
```

## 🚀 性能优化技巧

### 1. 批量数据加载
```python
# 使用批量加载提高性能
loaded_data = loader.load_data_batch(data_config, placeholders, job_datetime)
```

### 2. 合理设置 TTL
```python
# 设置数据过期时间
connector.store_json_variable("test", "data", value, ttl=3600)  # 1小时过期
```

### 3. 使用连接池
```python
# Redis 连接器自动使用连接池
connector = RedisConnector(
    host='localhost',
    port=6379,
    db=0
)
```

## 🧪 运行测试

```bash
# 运行所有测试
python run_tests.py

# 运行特定测试
cd test && python test_densets.py

# 运行示例
python example_engine.py
python example_forecast.py
```

## 📞 获取帮助

- 📖 完整文档: 查看 `README.md`
- 🧪 测试示例: 查看 `test/` 目录
- 💡 使用示例: 查看 `example_*.py` 文件
- 🐛 问题反馈: 提交 Issue

## 🎯 下一步

1. 阅读完整的 `README.md` 了解所有功能
2. 查看 `example_forecast.py` 了解复杂场景的使用
3. 运行测试确保环境正常
4. 开始构建你的规则引擎应用！ 