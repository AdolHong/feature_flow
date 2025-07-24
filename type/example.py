"""
Type模块使用示例
展示如何使用数据校验、序列化和反序列化功能
"""

import pandas as pd
from .schema import ValueSchema, DataFrameSchema, ColumnSchema
from .validator import DataValidator, SchemaValidator, MultiDataValidator
from .serializer import DataSerializer, DataDeserializer, DataSerializerManager


def example_value_validation():
    """单值数据校验示例"""
    print("=== 单值数据校验示例 ===")
    
    # 创建ValueSchema
    age_schema = ValueSchema('int')
    name_schema = ValueSchema('string')
    score_schema = ValueSchema('double')
    
    # 创建校验器
    age_validator = DataValidator(age_schema)
    name_validator = DataValidator(name_schema)
    score_validator = DataValidator(score_schema)
    
    # 测试数据
    test_data = [
        (25, age_validator, "年龄"),
        ("张三", name_validator, "姓名"),
        (85.5, score_validator, "分数"),
        ("not int", age_validator, "无效年龄"),
        (123, name_validator, "无效姓名"),
    ]
    
    for data, validator, description in test_data:
        is_valid = validator.validate(data)
        print(f"{description}: {data} -> {'✓' if is_valid else '✗'}")


def example_dataframe_validation():
    """DataFrame数据校验示例"""
    print("\n=== DataFrame数据校验示例 ===")
    
    # 定义DataFrame schema
    schema_str = "ds:string,is_activate:int,predicted:double"
    schema = DataFrameSchema.from_string(schema_str)
    validator = DataValidator(schema)
    
    # 创建有效的DataFrame
    valid_df = pd.DataFrame({
        'ds': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'is_activate': [1, 0, 1],
        'predicted': [0.8, 0.6, 0.9]
    })
    
    # 创建无效的DataFrame（列名不匹配）
    invalid_df1 = pd.DataFrame({
        'date': ['2023-01-01'],
        'is_activate': [1],
        'predicted': [0.8]
    })
    
    # 创建无效的DataFrame（数据类型不匹配）
    invalid_df2 = pd.DataFrame({
        'ds': [123, 456],  # 应该是string
        'is_activate': [1, 0],
        'predicted': [0.8, 0.6]
    })
    
    # 测试校验
    test_dataframes = [
        (valid_df, "有效DataFrame"),
        (invalid_df1, "列名不匹配"),
        (invalid_df2, "数据类型不匹配")
    ]
    
    for df, description in test_dataframes:
        is_valid = validator.validate(df)
        print(f"{description}: {'✓' if is_valid else '✗'}")


def example_serialization():
    """数据序列化和反序列化示例"""
    print("\n=== 数据序列化和反序列化示例 ===")
    
    # 单值数据序列化
    print("单值数据序列化:")
    value_data = 42
    value_schema = "int"
    
    serialized = DataSerializerManager.serialize_with_schema(value_data, value_schema)
    print(f"原始数据: {value_data}")
    print(f"序列化后: {serialized}")
    
    deserialized = DataSerializerManager.deserialize_auto(serialized)
    print(f"反序列化后: {deserialized}")
    print(f"数据是否一致: {value_data == deserialized}")
    
    # DataFrame序列化
    print("\nDataFrame序列化:")
    df_data = pd.DataFrame({
        'ds': ['2023-01-01', '2023-01-02'],
        'is_activate': [1, 0],
        'predicted': [0.8, 0.6]
    })
    df_schema = "ds:string,is_activate:int,predicted:double"
    
    serialized_df = DataSerializerManager.serialize_with_schema(df_data, df_schema)
    print(f"原始DataFrame:\n{df_data}")
    print(f"序列化后: {serialized_df[:100]}...")  # 只显示前100个字符
    
    deserialized_df = DataSerializerManager.deserialize_auto(serialized_df)
    print(f"反序列化后:\n{deserialized_df}")
    print(f"数据是否一致: {df_data.equals(deserialized_df)}")


def example_multi_validation():
    """多数据校验示例"""
    print("\n=== 多数据校验示例 ===")
    
    # 定义多个校验器
    validators = {
        'age': DataValidator.from_string('int'),
        'name': DataValidator.from_string('string'),
        'score': DataValidator.from_string('double'),
        'is_active': DataValidator.from_string('boolean')
    }
    
    multi_validator = MultiDataValidator(validators)
    
    # 测试数据
    test_data = {
        'age': 25,
        'name': '张三',
        'score': 85.5,
        'is_active': True
    }
    
    # 校验所有数据
    results = multi_validator.validate_all(test_data)
    print("校验结果:")
    for field, is_valid in results.items():
        print(f"  {field}: {'✓' if is_valid else '✗'}")
    
    # 测试部分无效数据
    invalid_data = {
        'age': 'not int',
        'name': '张三',
        'score': 85.5,
        'is_active': 'not boolean'
    }
    
    results = multi_validator.validate_all(invalid_data)
    print("\n部分无效数据校验结果:")
    for field, is_valid in results.items():
        print(f"  {field}: {'✓' if is_valid else '✗'}")
    
    # 获取无效和有效的数据源
    invalid_sources = multi_validator.get_invalid_data(invalid_data)
    valid_sources = multi_validator.get_valid_data(invalid_data)
    
    print(f"\n无效数据源: {invalid_sources}")
    print(f"有效数据源: {valid_sources}")


def example_schema_validation():
    """Schema字符串校验示例"""
    print("\n=== Schema字符串校验示例 ===")
    
    # 测试有效的schema字符串
    valid_schemas = [
        'int',
        'string',
        'double',
        'boolean',
        'ds:string,is_activate:int',
        'name:string,age:int,score:double'
    ]
    
    print("有效的Schema字符串:")
    for schema_str in valid_schemas:
        is_valid = SchemaValidator.validate_schema_string(schema_str)
        schema_type = SchemaValidator.detect_schema_type(schema_str)
        print(f"  {schema_str} -> {schema_type} {'✓' if is_valid else '✗'}")
    
    # 测试无效的schema字符串
    invalid_schemas = [
        '',
        'invalid_type',
        'ds:invalid_type',
        'ds:string,invalid_column'
    ]
    
    print("\n无效的Schema字符串:")
    for schema_str in invalid_schemas:
        is_valid = SchemaValidator.validate_schema_string(schema_str)
        print(f"  {schema_str} -> {'✓' if is_valid else '✗'}")


def example_validate_and_serialize():
    """校验并序列化示例"""
    print("\n=== 校验并序列化示例 ===")
    
    # 测试有效数据
    valid_data = 42
    schema_str = 'int'
    
    serialized, is_valid = DataSerializerManager.validate_and_serialize(valid_data, schema_str)
    print(f"有效数据 {valid_data} (schema: {schema_str}):")
    print(f"  校验结果: {'✓' if is_valid else '✗'}")
    print(f"  序列化结果: {serialized}")
    
    # 测试无效数据
    invalid_data = "not int"
    
    serialized, is_valid = DataSerializerManager.validate_and_serialize(invalid_data, schema_str)
    print(f"\n无效数据 {invalid_data} (schema: {schema_str}):")
    print(f"  校验结果: {'✓' if is_valid else '✗'}")
    print(f"  序列化结果: {serialized}")


def main():
    """主函数，运行所有示例"""
    print("Type模块功能演示")
    print("=" * 50)
    
    example_value_validation()
    example_dataframe_validation()
    example_serialization()
    example_multi_validation()
    example_schema_validation()
    example_validate_and_serialize()
    
    print("\n" + "=" * 50)
    print("演示完成！")


if __name__ == '__main__':
    main() 