# TS-Rule-Backend

一个基于 Python 的规则引擎后端系统，支持从 Redis 加载数据并进行复杂的数据处理和分析。

## 🚀 主要功能

### 1. 数据加载 (Data Loading)
- 支持从 Redis 加载多种类型的数据
- 支持批量查询优化性能
- 支持占位符和动态参数替换
- 支持时间序列数据处理

### 2. 规则引擎 (Rule Engine)
- 支持四种节点类型：start、logic、gate、collection
- 支持节点间的依赖关系管理
- 支持数据流追踪和验证

### 3. 数据类型支持
- **value**: 简单值类型 (int, double, string, boolean)
- **json**: JSON 对象类型
- **timeseries**: 时间序列数据 (DataFrame)
- **densets**: 分隔符分隔的数据 (DataFrame)

### 4. 数据验证 (Data Validation)
- 支持类型转换和检查
- 支持 DataFrame 列类型验证

## 📁 项目结构

```
ts-rule-backend/
├── core/                    # 规则引擎核心
│   ├── engine.py           # 规则引擎主类
│   ├── nodes.py            # 节点定义
│   ├── data_flow.py        # 数据流管理
│   └── json_helper.py      # JSON 工具
├── data/                   # 数据加载模块
│   ├── dataloader.py       # 数据加载器
│   └── redis_connector.py  # Redis 连接器
├── type/                   # 类型系统
│   ├── schema.py           # Schema 定义
│   ├── validator.py        # 数据验证器
│   └── serializer.py       # 序列化工具
├── utils/                  # 工具模块
│   └── datetime_parser.py  # 日期时间解析
├── test/                   # 测试文件
├── example_forecast.py     # 预测示例
├── example_engine.py       # 引擎示例
└── requirements.txt        # 依赖包
```

## 🛠️ 安装和设置

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 启动 Redis
```bash
# macOS
brew services start redis

# Ubuntu
sudo systemctl start redis

# Docker
docker run -p 6379:6379 redis:latest
```

## 📖 使用指南

### 1. 数据配置 (Data Config)

数据配置定义了从 Redis 加载数据的规则：

```python
data_config = {
    "namespace": "sams_cloud_demand_fcst_tn",
    "placeholder": [
        {"name": "store_nbr", "description": "门店号"},
        {"name": "item_nbr", "description": "商品号"},
    ],
    "variable": [
        {
            "name": "dim_store",
            "description": "门店维度信息",
            "namespace": "sams_cloud",
            "redis_config": {
                "prefix": [
                    {"key": "store_nbr", "value": "${store_nbr}"}
                ],
                "field": "dim_store",
                "type": "json"
            }
        },
        {
            "name": "forecast_data",
            "description": "预测数据",
            "redis_config": {
                "prefix": [
                    {"key": "store_nbr", "value": "${store_nbr}"},
                    {"key": "item_nbr", "value": "${item_nbr}"}
                ],
                "field": "forecast_data",
                "type": "densets",
                "split": ",",
                "schema": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string"
            }
        }
    ]
}
```

### 2. 规则引擎 (Rule Engine)

创建和执行规则引擎：

```python
from core.engine import RuleEngine
from core.nodes import LogicNode, GateNode, CollectionNode

# 创建引擎
engine = RuleEngine("my_engine")

# 创建节点
logic_node = LogicNode("process_data")
logic_node.set_logic("""
# 处理数据
result = input_data * 2
trend = result.mean()
""")
logic_node.set_tracked_variables(["result", "trend"])

# 添加依赖关系
engine.add_dependency(None, logic_node)

# 执行引擎
results = engine.execute(
    job_date="2025-01-01 00:00:00",
    placeholders={"store_nbr": "123", "item_nbr": "456"},
    variables=loaded_data
)
```

### 3. 节点类型

#### StartNode
起始节点，自动创建：
```python
# 自动创建，无需手动配置
```

#### LogicNode
逻辑处理节点：
```python
logic_node = LogicNode("process")
logic_node.set_logic("""
# Python 代码
result = data * 2
""")
logic_node.set_tracked_variables(["result"])
```

#### GateNode
条件判断节点：
```python
gate_node = GateNode("check_condition")
gate_node.set_condition("value > 100")
```

#### CollectionNode
数据收集节点：
```python
collection_node = CollectionNode("collect")
collection_node.set_logic("""
# 收集上游节点的输出
import pandas as pd
df_concat = pd.concat([collection[node_name]['output'] for node_name in collection])
output = df_concat.groupby('date').sum()
""")
collection_node.set_tracked_variables(["output"])
```

### 4. 数据类型

#### Value 类型
```python
# 简单值
"type": "value"
```

#### JSON 类型
```python
# JSON 对象
"type": "json"
```

#### Timeseries 类型
```python
# 时间序列数据
"type": "timeseries",
"from_datetime": "${yyyy-MM-dd}",
"to_datetime": "${yyyy-MM-dd+6d}",
"drop_duplicate": "keep_latest"  # "none" 或 "keep_latest"
```

#### Densets 类型
```python
# 分隔符分隔的数据
"type": "densets",
"split": ",",
"schema": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string"
```

### 5. Schema 验证

支持两种 schema 格式：

#### 简单值 Schema
```python
"trend": "double"
"name": "string"
"count": "int"
"active": "boolean"
```

#### DataFrame Schema
```python
"forecast": "ds:string,fcst_qty:double,etl_time:string"
```

## 🔧 高级功能

### 1. 批量数据加载
```python
# 使用批量加载提高性能
loaded_data = loader.load_data_batch(data_config, placeholders, job_datetime)
```

### 2. 动态参数替换
支持多种动态参数：
- `${yyyy-MM-dd}`: 当前日期
- `${yyyy-MM-dd+6d}`: 6天后
- `${yyyy-MM-dd-1d}`: 1天前
- `${store_nbr}`: 占位符替换

### 3. 数据流追踪
```python
# 获取节点的数据流上下文
context = engine.get_node_flow_context("node_name")
values = engine.get_node_flow_context_values("node_name")
```

### 4. 执行摘要
```python
summary = engine.get_execution_summary()
print(f"成功率: {summary['success_rate']:.2%}")
```

## 📝 示例

### 完整示例
参考 `example_forecast.py` 查看完整的使用示例。

### 简单示例
```python
from data.redis_connector import RedisConnector
from data.dataloader import DataLoader
from core.engine import RuleEngine
from core.nodes import LogicNode

# 初始化
connector = RedisConnector()
loader = DataLoader(connector)
engine = RuleEngine("demo")

# 加载数据
data_config = {...}  # 你的数据配置
placeholders = {"store_nbr": "123"}
loaded_data = loader.load_data(data_config, placeholders, "2025-01-01 00:00:00")

# 创建规则
node = LogicNode("process")
node.set_logic("result = data * 2")
engine.add_dependency(None, node)

# 执行
results = engine.execute("2025-01-01 00:00:00", placeholders, loaded_data)
```

## 🧪 测试

运行测试：
```bash
# 运行所有测试
python run_tests.py

# 运行特定测试
cd test && python test_densets.py

# 使用 pytest
pytest test/ -v
```

## 📊 性能优化

1. **批量查询**: 使用 `load_data_batch()` 进行批量数据加载
2. **连接池**: Redis 连接器支持连接池
3. **缓存**: 支持数据缓存和 TTL 设置
4. **并行处理**: 规则引擎支持节点并行执行

## 🔍 故障排除

### 常见问题

1. **Redis 连接失败**
   - 检查 Redis 服务是否启动
   - 检查连接参数是否正确

2. **数据加载失败**
   - 检查 data_config 配置
   - 检查占位符是否正确替换
   - 检查 Redis 中是否存在对应数据

3. **规则引擎执行失败**
   - 检查节点依赖关系
   - 检查 schema 验证
   - 查看详细的错误日志

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📞 支持

如有问题，请提交 Issue 或联系开发团队。 