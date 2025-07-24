"""
Type模块测试文件
测试数据校验、序列化和反序列化功能
"""

import unittest
import pandas as pd
import json
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from type.schema import ValueSchema, DataFrameSchema, ColumnSchema
from type.validator import DataValidator, SchemaValidator, MultiDataValidator
from type.serializer import DataSerializer, DataDeserializer, DataSerializerManager


class TestValueSchema(unittest.TestCase):
    """测试ValueSchema"""
    
    def test_value_schema_creation(self):
        """测试ValueSchema创建"""
        # 测试有效类型
        valid_types = ['int', 'double', 'string', 'boolean', 'dict', 'list', 'None']
        for value_type in valid_types:
            schema = ValueSchema(value_type)
            self.assertEqual(schema.value_type, value_type)
        
        # 测试无效类型
        with self.assertRaises(ValueError):
            ValueSchema('invalid_type')
    
    def test_value_schema_validation(self):
        """测试ValueSchema数据校验"""
        # 测试int类型
        schema = ValueSchema('int')
        self.assertTrue(schema.validate(42))
        self.assertTrue(schema.validate(0))
        self.assertFalse(schema.validate(3.14))
        self.assertFalse(schema.validate("42"))
        
        # 测试double类型
        schema = ValueSchema('double')
        self.assertTrue(schema.validate(3.14))
        self.assertTrue(schema.validate(42))
        self.assertFalse(schema.validate("3.14"))
        
        # 测试string类型
        schema = ValueSchema('string')
        self.assertTrue(schema.validate("hello"))
        self.assertTrue(schema.validate(""))
        self.assertFalse(schema.validate(42))
        
        # 测试boolean类型
        schema = ValueSchema('boolean')
        self.assertTrue(schema.validate(True))
        self.assertTrue(schema.validate(False))
        self.assertFalse(schema.validate(1))
        
        # 测试dict类型
        schema = ValueSchema('dict')
        self.assertTrue(schema.validate({"key": "value"}))
        self.assertFalse(schema.validate([]))
        
        # 测试list类型
        schema = ValueSchema('list')
        self.assertTrue(schema.validate([1, 2, 3]))
        self.assertTrue(schema.validate([]))
        self.assertFalse(schema.validate({}))
        
        # 测试None类型
        schema = ValueSchema('None')
        self.assertTrue(schema.validate(None))
        self.assertFalse(schema.validate(0))
    
    def test_value_schema_serialization(self):
        """测试ValueSchema序列化"""
        schema = ValueSchema('int')
        
        # 测试to_dict
        schema_dict = schema.to_dict()
        self.assertEqual(schema_dict['type'], 'value')
        self.assertEqual(schema_dict['value_type'], 'int')
        
        # 测试from_dict
        new_schema = ValueSchema.from_dict(schema_dict)
        self.assertEqual(new_schema.value_type, 'int')
        
        # 测试to_string
        schema_str = schema.to_string()
        self.assertEqual(schema_str, 'int')
        
        # 测试from_string
        new_schema = ValueSchema.from_string(schema_str)
        self.assertEqual(new_schema.value_type, 'int')


class TestDataFrameSchema(unittest.TestCase):
    """测试DataFrameSchema"""
    
    def test_dataframe_schema_creation(self):
        """测试DataFrameSchema创建"""
        columns = [
            ColumnSchema('ds', 'string'),
            ColumnSchema('is_activate', 'int'),
            ColumnSchema('predicted', 'double')
        ]
        schema = DataFrameSchema(columns)
        self.assertEqual(len(schema.columns), 3)
    
    def test_dataframe_schema_validation(self):
        """测试DataFrameSchema数据校验"""
        columns = [
            ColumnSchema('ds', 'string'),
            ColumnSchema('is_activate', 'int'),
            ColumnSchema('predicted', 'double')
        ]
        schema = DataFrameSchema(columns)
        
        # 创建测试DataFrame
        df = pd.DataFrame({
            'ds': ['2023-01-01', '2023-01-02'],
            'is_activate': [1, 0],
            'predicted': [0.8, 0.6]
        })
        
        self.assertTrue(schema.validate(df))
        
        # 测试列名不匹配
        df_wrong_columns = pd.DataFrame({
            'date': ['2023-01-01'],
            'is_activate': [1],
            'predicted': [0.8]
        })
        self.assertFalse(schema.validate(df_wrong_columns))
        
        # 测试数据类型不匹配
        df_wrong_types = pd.DataFrame({
            'ds': [123, 456],  # 应该是string
            'is_activate': [1, 0],
            'predicted': [0.8, 0.6]
        })
        self.assertFalse(schema.validate(df_wrong_types))
    
    def test_dataframe_schema_serialization(self):
        """测试DataFrameSchema序列化"""
        columns = [
            ColumnSchema('ds', 'string'),
            ColumnSchema('is_activate', 'int'),
            ColumnSchema('predicted', 'double')
        ]
        schema = DataFrameSchema(columns)
        
        # 测试to_dict
        schema_dict = schema.to_dict()
        self.assertEqual(schema_dict['type'], 'dataframe')
        self.assertEqual(len(schema_dict['columns']), 3)
        
        # 测试from_dict
        new_schema = DataFrameSchema.from_dict(schema_dict)
        self.assertEqual(len(new_schema.columns), 3)
        
        # 测试to_string
        schema_str = schema.to_string()
        expected = "ds:string,is_activate:int,predicted:double"
        self.assertEqual(schema_str, expected)
        
        # 测试from_string
        new_schema = DataFrameSchema.from_string(schema_str)
        self.assertEqual(len(new_schema.columns), 3)
        self.assertEqual(new_schema.columns[0].name, 'ds')
        self.assertEqual(new_schema.columns[0].type, 'string')


class TestSchemaValidator(unittest.TestCase):
    """测试SchemaValidator"""
    
    def test_validate_schema_string(self):
        """测试schema字符串校验"""
        # 测试有效的ValueSchema
        self.assertTrue(SchemaValidator.validate_schema_string('int'))
        self.assertTrue(SchemaValidator.validate_schema_string('string'))
        
        # 测试有效的DataFrameSchema
        self.assertTrue(SchemaValidator.validate_schema_string('ds:string,is_activate:int'))
        
        # 测试无效的schema
        self.assertFalse(SchemaValidator.validate_schema_string(''))
        self.assertFalse(SchemaValidator.validate_schema_string('invalid'))
        self.assertFalse(SchemaValidator.validate_schema_string('ds:invalid_type'))
    
    def test_detect_schema_type(self):
        """测试schema类型检测"""
        # 测试ValueSchema
        self.assertEqual(SchemaValidator.detect_schema_type('int'), 'value')
        self.assertEqual(SchemaValidator.detect_schema_type('string'), 'value')
        
        # 测试DataFrameSchema
        self.assertEqual(SchemaValidator.detect_schema_type('ds:string'), 'dataframe')
        self.assertEqual(SchemaValidator.detect_schema_type('ds:string,is_activate:int'), 'dataframe')
    
    def test_parse_schema(self):
        """测试schema解析"""
        # 测试ValueSchema解析
        schema = SchemaValidator.parse_schema('int')
        self.assertIsInstance(schema, ValueSchema)
        self.assertEqual(schema.value_type, 'int')
        
        # 测试DataFrameSchema解析
        schema = SchemaValidator.parse_schema('ds:string,is_activate:int')
        self.assertIsInstance(schema, DataFrameSchema)
        self.assertEqual(len(schema.columns), 2)


class TestDataValidator(unittest.TestCase):
    """测试DataValidator"""
    
    def test_data_validator_creation(self):
        """测试DataValidator创建"""
        schema = ValueSchema('int')
        validator = DataValidator(schema)
        self.assertEqual(validator.schema, schema)
    
    def test_data_validator_validation(self):
        """测试DataValidator数据校验"""
        schema = ValueSchema('int')
        validator = DataValidator(schema)
        
        self.assertTrue(validator.validate(42))
        self.assertFalse(validator.validate("42"))
    
    def test_data_validator_from_string(self):
        """测试从字符串创建DataValidator"""
        validator = DataValidator.from_string('int')
        self.assertIsInstance(validator.schema, ValueSchema)
        self.assertEqual(validator.schema.value_type, 'int')
    
    def test_data_validator_from_dict(self):
        """测试从字典创建DataValidator"""
        schema_dict = {'type': 'value', 'value_type': 'string'}
        validator = DataValidator.from_dict(schema_dict)
        self.assertIsInstance(validator.schema, ValueSchema)
        self.assertEqual(validator.schema.value_type, 'string')


class TestDataSerializer(unittest.TestCase):
    """测试DataSerializer"""
    
    def test_value_serialization(self):
        """测试单值数据序列化"""
        schema = ValueSchema('int')
        serializer = DataSerializer(schema)
        
        # 序列化
        serialized = serializer.serialize(42)
        data_dict = json.loads(serialized)
        
        self.assertEqual(data_dict['type'], 'value')
        self.assertEqual(data_dict['schema'], 'int')
        self.assertEqual(data_dict['data'], 42)
        
        # 反序列化
        deserializer = DataDeserializer(schema)
        deserialized = deserializer.deserialize(serialized)
        self.assertEqual(deserialized, 42)
    
    def test_dataframe_serialization(self):
        """测试DataFrame序列化"""
        columns = [
            ColumnSchema('ds', 'string'),
            ColumnSchema('is_activate', 'int')
        ]
        schema = DataFrameSchema(columns)
        serializer = DataSerializer(schema)
        
        # 创建测试DataFrame
        df = pd.DataFrame({
            'ds': ['2023-01-01', '2023-01-02'],
            'is_activate': [1, 0]
        })
        
        # 序列化
        serialized = serializer.serialize(df)
        data_dict = json.loads(serialized)
        
        self.assertEqual(data_dict['type'], 'dataframe')
        self.assertEqual(data_dict['schema'], 'ds:string,is_activate:int')
        self.assertIsInstance(data_dict['data'], list)
        
        # 反序列化
        deserializer = DataDeserializer(schema)
        deserialized = deserializer.deserialize(serialized)
        self.assertIsInstance(deserialized, pd.DataFrame)
        self.assertEqual(len(deserialized), 2)
    
    def test_serialization_with_invalid_data(self):
        """测试无效数据的序列化"""
        schema = ValueSchema('int')
        serializer = DataSerializer(schema)
        
        with self.assertRaises(ValueError):
            serializer.serialize("not an int")


class TestDataSerializerManager(unittest.TestCase):
    """测试DataSerializerManager"""
    
    def test_serialize_with_schema(self):
        """测试使用schema字符串序列化"""
        serialized = DataSerializerManager.serialize_with_schema(42, 'int')
        data_dict = json.loads(serialized)
        self.assertEqual(data_dict['data'], 42)
    
    def test_deserialize_with_schema(self):
        """测试使用schema字符串反序列化"""
        serialized = DataSerializerManager.serialize_with_schema(42, 'int')
        deserialized = DataSerializerManager.deserialize_with_schema(serialized, 'int')
        self.assertEqual(deserialized, 42)
    
    def test_deserialize_auto(self):
        """测试自动反序列化"""
        serialized = DataSerializerManager.serialize_with_schema(42, 'int')
        deserialized = DataSerializerManager.deserialize_auto(serialized)
        self.assertEqual(deserialized, 42)
    
    def test_validate_and_serialize(self):
        """测试校验并序列化"""
        # 测试有效数据
        serialized, is_valid = DataSerializerManager.validate_and_serialize(42, 'int')
        self.assertTrue(is_valid)
        self.assertNotEqual(serialized, "")
        
        # 测试无效数据
        serialized, is_valid = DataSerializerManager.validate_and_serialize("not int", 'int')
        self.assertFalse(is_valid)
        self.assertEqual(serialized, "")


class TestMultiDataValidator(unittest.TestCase):
    """测试MultiDataValidator"""
    
    def test_multi_data_validator(self):
        """测试多数据校验器"""
        validators = {
            'age': DataValidator.from_string('int'),
            'name': DataValidator.from_string('string'),
            'score': DataValidator.from_string('double')
        }
        
        multi_validator = MultiDataValidator(validators)
        
        # 测试数据
        data_dict = {
            'age': 25,
            'name': 'John',
            'score': 85.5
        }
        
        # 校验所有数据
        results = multi_validator.validate_all(data_dict)
        self.assertTrue(all(results.values()))
        
        # 测试部分无效数据
        invalid_data = {
            'age': 'not int',
            'name': 'John',
            'score': 85.5
        }
        
        results = multi_validator.validate_all(invalid_data)
        self.assertFalse(results['age'])
        self.assertTrue(results['name'])
        self.assertTrue(results['score'])
        
        # 获取无效数据源
        invalid_sources = multi_validator.get_invalid_data(invalid_data)
        self.assertEqual(invalid_sources, ['age'])
        
        # 获取有效数据源
        valid_sources = multi_validator.get_valid_data(invalid_data)
        self.assertEqual(set(valid_sources), {'name', 'score'})


if __name__ == '__main__':
    unittest.main() 