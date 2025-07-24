# Type模块

Type模块提供了数据格式校验、序列化和反序列化功能，支持单值数据和DataFrame两种数据类型。

## 功能特性

### 1. 数据格式定义 (Schema)
- **ValueSchema**: 支持单值数据类型 (int, double, string, boolean, dict, list, None)
- **DataFrameSchema**: 支持DataFrame数据格式，可定义列名和数据类型

### 2. 数据校验 (Validator)
- **DataValidator**: 单个数据校验器
- **MultiDataValidator**: 多数据校验器，支持批量校验
- **SchemaValidator**: Schema字符串校验和解析

### 3. 序列化和反序列化 (Serializer)
- **DataSerializer**: 数据序列化器
- **DataDeserializer**: 数据反序列化器
- **DataSerializerManager**: 便捷的序列化管理器

## 快速开始

### 1. 单值数据校验

```python
from type.validator import DataValidator
from type.schema import ValueSchema

# 创建校验器
validator = DataValidator.from_string('int')

# 校验数据
is_valid = validator.validate(42)  # True
is_valid = validator.validate("42")  # False
```

### 2. DataFrame数据校验

```python
from type.validator import DataValidator

# 定义DataFrame schema
schema_str = "ds:string,is_activate:int,predicted:double"
validator = DataValidator.from_string(schema_str)

# 创建测试DataFrame
import pandas as pd
df = pd.DataFrame({
    'ds': ['2023-01-01', '2023-01-02'],
    'is_activate': [1, 0],
    'predicted': [0.8, 0.6]
})

# 校验数据
is_valid = validator.validate(df)  # True
```

### 3. 数据序列化

```python
from type.serializer import DataSerializerManager

# 序列化单值数据
value_data = 42
schema_str = "int"
serialized = DataSerializerManager.serialize_with_schema(value_data, schema_str)

# 序列化DataFrame数据
df_data = pd.DataFrame({
    'ds': ['2023-01-01', '2023-01-02'],
    'is_activate': [1, 0],
    'predicted': [0.8, 0.6]
})
df_schema = "ds:string,is_activate:int,predicted:double"
serialized_df = DataSerializerManager.serialize_with_schema(df_data, df_schema)
```

### 4. 数据反序列化

```python
from type.serializer import DataSerializerManager

# 自动反序列化（从序列化数据中获取schema）
deserialized = DataSerializerManager.deserialize_auto(serialized)

# 指定schema反序列化
deserialized = DataSerializerManager.deserialize_with_schema(serialized, schema_str)
```

### 5. 多数据校验

```python
from type.validator import MultiDataValidator, DataValidator

# 定义多个校验器
validators = {
    'age': DataValidator.from_string('int'),
    'name': DataValidator.from_string('string'),
    'score': DataValidator.from_string('double')
}

multi_validator = MultiDataValidator(validators)

# 校验所有数据
data_dict = {
    'age': 25,
    'name': '张三',
    'score': 85.5
}

results = multi_validator.validate_all(data_dict)
# 结果: {'age': True, 'name': True, 'score': True}

# 获取无效数据源
invalid_sources = multi_validator.get_invalid_data(data_dict)
```

## Schema格式说明

### ValueSchema格式
单值数据的schema格式非常简单，直接使用数据类型名称：

```
int          # 整数类型
double       # 浮点数类型
string       # 字符串类型
boolean      # 布尔类型
dict         # 字典类型
list         # 列表类型
None         # 空值类型
```

### DataFrameSchema格式
DataFrame的schema格式为：`列名:类型,列名:类型,...`

```
ds:string,is_activate:int,predicted:double
```

支持的列类型：
- `string`: 字符串类型
- `int`: 整数类型
- `double`: 浮点数类型
- `boolean`: 布尔类型

## 序列化格式

序列化后的数据为JSON格式，包含以下字段：

```json
{
    "type": "value|dataframe",
    "schema": "schema字符串",
    "data": "实际数据"
}
```

### 单值数据示例
```json
{
    "type": "value",
    "schema": "int",
    "data": 42
}
```

### DataFrame数据示例
```json
{
    "type": "dataframe",
    "schema": "ds:string,is_activate:int,predicted:double",
    "data": [
        {"ds": "2023-01-01", "is_activate": 1, "predicted": 0.8},
        {"ds": "2023-01-02", "is_activate": 0, "predicted": 0.6}
    ]
}
```

## 高级用法

### 1. 自定义Schema对象

```python
from type.schema import ValueSchema, DataFrameSchema, ColumnSchema

# 创建ValueSchema
value_schema = ValueSchema('int')

# 创建DataFrameSchema
columns = [
    ColumnSchema('ds', 'string'),
    ColumnSchema('is_activate', 'int'),
    ColumnSchema('predicted', 'double')
]
df_schema = DataFrameSchema(columns)
```

### 2. Schema字符串校验

```python
from type.validator import SchemaValidator

# 校验schema字符串格式
is_valid = SchemaValidator.validate_schema_string('int')  # True
is_valid = SchemaValidator.validate_schema_string('invalid')  # False

# 检测schema类型
schema_type = SchemaValidator.detect_schema_type('int')  # 'value'
schema_type = SchemaValidator.detect_schema_type('ds:string')  # 'dataframe'
```

### 3. 校验并序列化

```python
from type.serializer import DataSerializerManager

# 校验并序列化，返回(序列化结果, 是否校验成功)
serialized, is_valid = DataSerializerManager.validate_and_serialize(data, schema_str)
```

## 错误处理

模块提供了完善的错误处理机制：

1. **Schema格式错误**: 当schema字符串格式不正确时抛出`ValueError`
2. **数据类型错误**: 当数据不符合schema定义时抛出`ValueError`
3. **序列化错误**: 当序列化失败时抛出`ValueError`
4. **反序列化错误**: 当反序列化失败时抛出`ValueError`

## 测试

运行测试：

```bash
python -m pytest test/test_type_validation.py -v
```

运行示例：

```bash
python -c "from type.example import main; main()"
```

## 前端兼容性

本模块设计的序列化格式与JavaScript前端完全兼容：

1. **JSON格式**: 使用标准JSON格式，前端可直接解析
2. **Schema字符串**: 前端可以使用相同的schema字符串格式进行校验
3. **数据类型**: 支持的数据类型与JavaScript原生类型对应

前端可以轻松实现相同的数据校验和序列化功能。 