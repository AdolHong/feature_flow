# densets 数据类型功能总结

## 概述

`densets` 是一种新的数据类型，用于存储 DataFrame 数据。与 `timeseries` 不同，`densets` 将每行数据转换为分隔符分隔的字符串存储，而不是 JSON 格式。

## 存储格式

### 原始数据示例
```python
test_data = [
    {"job_date": "2025-07-23", "fcst_date": "2025-07-23", "fcst_qty": 6.24, "etl_time": "2025-07-23 14:49:45"},
    {"job_date": "2025-07-23", "fcst_date": "2025-07-24", "fcst_qty": 6.44, "etl_time": "2025-07-23 14:49:45"},
    # ...
]
```

### 存储格式
每行数据被转换为分隔符分隔的字符串：
```
2025-07-23,2025-07-23,6.24,2025-07-23 14:49:45
2025-07-23,2025-07-24,6.44,2025-07-23 14:49:45
```

## 配置参数

### Redis 配置
```json
{
    "type": "densets",
    "field": "forecast_data",
    "prefix": [
        {"key": "env", "value": "${env}"},
        {"key": "region", "value": "${region}"}
    ],
    "split": ",",                    // 分隔符，默认为 ","
    "schema": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string",  // schema字符串
    "drop_duplicate": "none"         // 去重策略："none" 或 "keep_latest"
}
```

### 参数说明
- `split`: 分隔符，默认为 `,`
- `schema`: schema字符串，定义列名和类型，格式为 `"列名:类型,列名:类型,..."`
  - 支持的类型：`string`, `int`, `double`, `boolean`
  - 例如：`"job_date:string,fcst_date:string,fcst_qty:double,etl_time:string"`
- `drop_duplicate`: 去重策略
  - `none`: 不做去重处理
  - `keep_latest`: 保留最新的记录

## Schema 验证规则

### 数量验证
- 如果 Redis 中分割后的值数量**少于** schema 中定义的列数量，会报错
- 如果 Redis 中分割后的值数量**多于** schema 中定义的列数量，可以接受（只使用前N个值）

### 类型验证
- 系统会根据 schema 定义的类型进行自动转换和验证
- 如果类型转换失败，会报错并返回 `None`

### 支持的类型转换
- `string`: 保持原始字符串
- `int`: 转换为整数
- `double`: 转换为浮点数
- `boolean`: 转换为布尔值（支持 `true/false`, `1/0`, `yes/no`, `on/off`）

## 实现的功能

### RedisConnector 新增方法
1. `add_densets_point()` - 添加 densets 数据点
2. `get_densets_range()` - 获取 densets 数据范围
3. `get_densets_latest()` - 获取最新 densets 数据
4. `get_densets_count()` - 获取 densets 数据点数量
5. `remove_densets_range()` - 删除指定时间范围的 densets 数据
6. `cleanup_old_densets()` - 清理旧的 densets 数据

### DataLoader 新增方法
1. `_load_densets()` - 加载 densets 数据
2. `_get_densets_data()` - 从 Redis 获取 densets 数据
3. `_convert_densets_to_dataframe()` - 将 densets 数据转换为 DataFrame
4. `_parse_schema_string()` - 解析 schema 字符串
5. `_convert_value_type()` - 类型转换和验证

## 使用示例

### 存储数据
```python
# 创建测试数据
test_data = [
    {"job_date": "2025-07-23", "fcst_date": "2025-07-23", "fcst_qty": 6.24, "etl_time": "2025-07-23 14:49:45"},
    {"job_date": "2025-07-23", "fcst_date": "2025-07-24", "fcst_qty": 6.44, "etl_time": "2025-07-23 14:49:45"},
    # ...
]

df = pd.DataFrame(test_data)

# 存储数据
for i, row in df.iterrows():
    row_dict = row.to_dict()
    timestamp = base_timestamp + i * 86400
    redis_connector.add_densets_point(namespace, series_key, timestamp, row_dict)
```

### 加载数据
```python
# 数据配置
data_config = {
    "namespace": "test_densets",
    "placeholder": [
        {"name": "env", "value": "prod"},
        {"name": "region", "value": "cn"}
    ],
    "variable": [
        {
            "name": "forecast_data",
            "redis_config": {
                "type": "densets",
                "field": "forecast_data",
                "prefix": [
                    {"key": "env", "value": "${env}"},
                    {"key": "region", "value": "${region}"}
                ],
                "split": ",",
                "schema": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string",
                "drop_duplicate": "none"
            }
        }
    ]
}

# 加载数据
placeholders = {"env": "prod", "region": "cn"}
job_datetime = "2025-07-23 14:49:45"
loaded_data = data_loader.load_data(data_config, placeholders, job_datetime)
```

## 错误处理

### 数量不足错误
```
数据值数量不足: 期望至少 6 个值，实际只有 5 个值。数据: 2025-07-23,2025-07-23,6.24,2025-07-23 14:49:45,True
```

### 类型转换错误
```
无法将值 'not_a_number' 转换为 double 类型
```

### 处理方式
- 当发生错误时，对应的变量值会被设置为 `None`
- 其他变量仍然会正常加载
- 错误信息会通过日志输出

## 优势

1. **存储效率**: 相比 JSON 格式，分隔符分隔的字符串更节省存储空间
2. **解析速度**: 字符串分割比 JSON 解析更快
3. **类型安全**: 支持 schema 定义和类型验证
4. **灵活性**: 支持自定义分隔符和 schema 映射
5. **兼容性**: 与现有的时间序列功能完全兼容
6. **容错性**: 支持值数量多于 schema 定义的情况

## 注意事项

1. 确保 `schema` 配置与数据字段顺序一致
2. 分隔符不能出现在数据值中
3. 所有字段值都会被转换为字符串存储
4. 时间戳用于排序和去重，确保时间戳的唯一性
5. 类型转换失败会导致整个变量加载失败（返回 `None`）
6. 值数量不足会导致整个变量加载失败（返回 `None`） 