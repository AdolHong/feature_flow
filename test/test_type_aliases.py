"""
测试类型别名支持
"""

import pytest
import pandas as pd
from type.schema import ColumnSchema, DataFrameSchema, normalize_type, TYPE_ALIASES


class TestTypeAliases:
    """测试类型别名功能"""
    
    def test_normalize_type(self):
        """测试类型标准化"""
        # 测试字符串类型
        assert normalize_type('str') == 'string'
        assert normalize_type('string') == 'string'
        assert normalize_type('STR') == 'string'
        assert normalize_type('String') == 'string'
        
        # 测试整数类型
        assert normalize_type('int') == 'int'
        assert normalize_type('INT') == 'int'
        
        # 测试浮点数类型
        assert normalize_type('float') == 'double'
        assert normalize_type('double') == 'double'
        assert normalize_type('FLOAT') == 'double'
        assert normalize_type('Double') == 'double'
        
        # 测试布尔类型
        assert normalize_type('bool') == 'boolean'
        assert normalize_type('boolean') == 'boolean'
        assert normalize_type('BOOL') == 'boolean'
        assert normalize_type('Boolean') == 'boolean'
        
        # 测试无效类型
        with pytest.raises(ValueError):
            normalize_type('invalid_type')
    
    def test_column_schema_with_aliases(self):
        """测试ColumnSchema支持类型别名"""
        # 测试各种类型别名
        col1 = ColumnSchema('name', 'str')
        assert col1.type == 'string'
        
        col2 = ColumnSchema('age', 'int')
        assert col2.type == 'int'
        
        col3 = ColumnSchema('score', 'float')
        assert col3.type == 'double'
        
        col4 = ColumnSchema('active', 'bool')
        assert col4.type == 'boolean'
        
        col5 = ColumnSchema('description', 'string')
        assert col5.type == 'string'
        
        col6 = ColumnSchema('price', 'double')
        assert col6.type == 'double'
        
        col7 = ColumnSchema('enabled', 'boolean')
        assert col7.type == 'boolean'
    
    def test_dataframe_schema_with_aliases(self):
        """测试DataFrameSchema支持类型别名"""
        # 创建包含类型别名的schema字符串
        schema_str = "name:str,age:int,score:float,active:bool,price:double,enabled:boolean"
        
        schema = DataFrameSchema.from_string(schema_str)
        
        # 验证列类型被正确标准化
        expected_types = ['string', 'int', 'double', 'boolean', 'double', 'boolean']
        for col, expected_type in zip(schema.columns, expected_types):
            assert col.type == expected_type
    
    def test_dataframe_validation_with_aliases(self):
        """测试DataFrame验证支持类型别名"""
        # 创建schema
        schema_str = "name:str,age:int,score:float,active:bool"
        schema = DataFrameSchema.from_string(schema_str)
        
        # 创建测试数据
        data = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'score': [85.5, 92.0, 78.5],
            'active': [True, False, True]
        })
        
        # 验证数据
        assert schema.validate(data)
    
    def test_all_type_aliases_supported(self):
        """测试所有类型别名都被支持"""
        # 验证TYPE_ALIASES包含所有需要的映射
        expected_aliases = {
            'str', 'string',
            'int',
            'float', 'double',
            'bool', 'boolean'
        }
        
        assert set(TYPE_ALIASES.keys()) == expected_aliases
        
        # 验证所有别名都能正确标准化
        for alias in expected_aliases:
            normalized = normalize_type(alias)
            assert normalized in {'string', 'int', 'double', 'boolean'}


if __name__ == '__main__':
    pytest.main([__file__]) 