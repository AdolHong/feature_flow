# 基于数据流的规则引擎

一个功能强大的基于数据流的规则引擎，支持五种节点类型和复杂的分支逻辑。

## 功能特性

### 🎯 五种节点类型

1. **输入节点 (InputNode)**
   - 接收全局参数 (placeholders)
   - 接收全局变量 (variables)
   - 将数据传递给下游节点

2. **判断节点 (JudgeNode)**
   - 执行Python代码进行条件判断
   - 支持多个分支，每个分支有独立的下游
   - 临时变量不会向下游传递

3. **逻辑节点 (LogicNode)**
   - 执行Python代码生产数据
   - 输出数据给下游节点

4. **Collection节点 (CollectionNode)**
   - 收集多个上游数据
   - 支持数据格式校验
   - 允许上游失败，只收集正常输出

5. **输出节点 (OutputNode)**
   - 接受指定格式的数据
   - 支持多种数据格式校验
   - 支持多个上游输入

### 🔄 数据流特性

- **分支依赖**: 判断节点可以有多个分支，每个分支有独立的下游
- **拓扑排序**: 自动计算节点执行顺序
- **循环依赖检测**: 防止循环依赖导致的死锁
- **数据格式校验**: 支持多种数据类型的验证
- **错误处理**: 优雅处理节点执行失败

## 安装依赖

```bash
pip install -r requirements.txt
```

## 快速开始

### 基本使用

```python
from core.engine import RuleEngine
from core.nodes import InputNode, JudgeNode, LogicNode, OutputNode

# 创建规则引擎
engine = RuleEngine("my_engine")

# 创建输入节点
input_node = InputNode(
    name="input",
    placeholders={"city": "beijing"},
    variables={"temperature": 25, "humidity": 60}
)
engine.add_node(input_node)

# 创建判断节点
judge_node = JudgeNode(name="judge")
judge_node.add_branch("temperature > 20", "warm", "warm_logic")
judge_node.add_branch("temperature <= 20", "cold", "cold_logic")
engine.add_node(judge_node)

# 创建逻辑节点
logic_node = LogicNode(name="warm_logic")
logic_node.set_logic("result = 'comfortable'")
engine.add_node(logic_node)

# 创建输出节点
output_node = OutputNode(name="output")
output_node.add_expected_format("result", "string")
engine.add_node(output_node)

# 添加依赖关系
engine.add_dependency("input", "judge")
engine.add_branch_dependency("judge", "warm", "warm_logic", "warm_branch")
engine.add_dependency("warm_logic", "output")

# 验证并执行
is_valid, errors = engine.validate()
if is_valid:
    results = engine.execute()
    print("执行成功!")
```

### 运行示例

```bash
# 运行完整示例
python run_rule_engine.py

# 运行测试
python -m pytest test/test_rule_engine.py -v
```

## 详细示例

### 销售分析引擎

```python
from core.example import create_sales_analysis_engine

# 创建销售分析引擎
engine = create_sales_analysis_engine()

# 执行引擎
results = engine.execute()

# 获取执行摘要
summary = engine.get_execution_summary()
print(f"成功率: {summary['success_rate']:.2%}")

# 获取最终输出
outputs = engine.get_final_outputs()
print(outputs)
```

### 天气分析引擎

```python
from core.example import create_weather_analysis_engine

# 创建天气分析引擎
engine = create_weather_analysis_engine()

# 执行引擎
results = engine.execute()

# 可视化数据流
print(engine.visualize_flow())
```

## 节点类型详解

### 输入节点 (InputNode)

```python
input_node = InputNode(
    name="input_data",
    placeholders={
        "city_en": "beijing",
        "date": "2024-01-15"
    },
    variables={
        "df_quantity": pd.DataFrame({
            'product': ['A', 'B', 'C'],
            'quantity': [100, 150, 80],
            'price': [10, 15, 20]
        })
    }
)
```

### 判断节点 (JudgeNode)

```python
judge_node = JudgeNode(name="sales_judge")
judge_node.add_branch(
    condition="df_quantity['quantity'].sum() > 200",
    alias="high_sales",
    downstream="high_sales_logic"
)
judge_node.add_branch(
    condition="df_quantity['quantity'].sum() <= 200",
    alias="low_sales", 
    downstream="low_sales_logic"
)
```

### 逻辑节点 (LogicNode)

```python
logic_node = LogicNode(name="high_sales_logic")
logic_node.set_logic("""
# 高销量逻辑处理
total_quantity = df_quantity['quantity'].sum()
total_revenue = (df_quantity['quantity'] * df_quantity['price']).sum()
avg_price = total_revenue / total_quantity

high_sales_result = {
    'type': 'high_sales',
    'total_quantity': total_quantity,
    'total_revenue': total_revenue,
    'avg_price': avg_price,
    'recommendation': 'Increase production capacity'
}
""")
```

### Collection节点 (CollectionNode)

```python
collection_node = CollectionNode(name="sales_collection")
# 自动收集符合格式 (node_name, score, value) 的上游数据
```

### 输出节点 (OutputNode)

```python
output_node = OutputNode(name="final_output")
output_node.add_expected_format("sales_data", "dict")
output_node.add_expected_format("city", "string")
output_node.add_expected_format("analysis_date", "string")
```

## 数据格式支持

### 基本类型
- `int`: 整数
- `string`: 字符串
- `double`: 浮点数
- `bool`: 布尔值
- `dict`: 字典
- `list`: 列表
- `dataframe`: Pandas DataFrame
- `None`: 空值

### 自定义Schema验证

```python
output_node.add_expected_format("user_data", "dict", {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer", "minimum": 0},
        "email": {"type": "string", "format": "email"}
    },
    "required": ["name", "age"]
})
```

## 分支依赖语法

### 基本依赖
```python
engine.add_dependency("source_node", "target_node")
```

### 分支依赖
```python
engine.add_branch_dependency(
    source_node="judge_node",
    condition="high_sales", 
    target_node="logic_node",
    alias="high_branch"
)
```

### 分支别名
```python
# 判断节点可以有多个分支
judge_node.add_branch("condition1", "alias1", "downstream1")
judge_node.add_branch("condition2", "alias2", "downstream2")
judge_node.add_branch("condition3", "alias3", "downstream3")
```

## 错误处理

### 验证错误
```python
is_valid, errors = engine.validate()
if not is_valid:
    print(f"验证失败: {errors}")
```

### 执行错误
```python
results = engine.execute()
for node_name, result in results.items():
    if not result.success:
        print(f"节点 {node_name} 执行失败: {result.error}")
```

## 监控和调试

### 执行摘要
```python
summary = engine.get_execution_summary()
print(f"总节点数: {summary['total_nodes']}")
print(f"成功节点数: {summary['successful_nodes']}")
print(f"失败节点数: {summary['failed_nodes']}")
print(f"成功率: {summary['success_rate']:.2%}")
```

### 可视化数据流
```python
print(engine.visualize_flow())
```

### 节点信息
```python
node_info = engine.get_node_info("node_name")
print(f"节点类型: {node_info['type']}")
print(f"依赖: {node_info['dependencies']}")
print(f"输出: {node_info['outputs']}")
```

## 项目结构

```
ts-rule-backend/
├── core/                    # 规则引擎核心模块
│   ├── __init__.py
│   ├── engine.py           # 规则引擎主类
│   ├── nodes.py            # 节点定义
│   ├── data_flow.py        # 数据流管理
│   └── example.py          # 使用示例
├── test/                   # 测试文件
│   └── test_rule_engine.py
├── data/                   # 数据处理模块
├── requirements.txt        # 依赖包
├── run_rule_engine.py      # 运行脚本
└── README.md              # 说明文档
```

## 开发指南

### 添加新节点类型

1. 继承 `Node` 基类
2. 实现 `execute` 方法
3. 在 `core/__init__.py` 中导出

### 扩展数据格式验证

1. 在 `OutputNode._validate_data_format` 中添加新类型
2. 支持自定义 schema 验证

### 性能优化

- 使用拓扑排序确保正确的执行顺序
- 支持并行执行（未来版本）
- 缓存中间结果（未来版本）

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！ 