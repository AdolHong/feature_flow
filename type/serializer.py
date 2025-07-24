"""
数据序列化和反序列化模块
提供数据序列化和反序列化功能，支持value和dataframe两种数据类型
"""

import json
import base64
import pickle
from typing import Any, Dict, List, Union, Optional, Tuple
import pandas as pd

from .schema import Schema, ValueSchema, DataFrameSchema
from .validator import DataValidator, SchemaValidator


class DataSerializer:
    """数据序列化器"""
    
    def __init__(self, schema: Schema):
        """
        初始化数据序列化器
        
        Args:
            schema: 数据格式定义
        """
        self.schema = schema
        self.validator = DataValidator(schema)
    
    def serialize(self, data: Any) -> str:
        """
        序列化数据为字符串
        
        Args:
            data: 待序列化的数据
            
        Returns:
            str: 序列化后的字符串
        """
        # 首先校验数据格式
        if not self.validator.validate(data):
            raise ValueError("数据格式不符合schema定义")
        
        # 根据schema类型进行序列化
        if isinstance(self.schema, ValueSchema):
            return self._serialize_value(data)
        elif isinstance(self.schema, DataFrameSchema):
            return self._serialize_dataframe(data)
        else:
            raise ValueError(f"不支持的schema类型: {type(self.schema)}")
    
    def _serialize_value(self, data: Any) -> str:
        """序列化单值数据"""
        # 使用JSON序列化，确保前端可以解析
        serialized = {
            'type': 'value',
            'schema': self.schema.to_string(),
            'data': data
        }
        return json.dumps(serialized, ensure_ascii=False)
    
    def _serialize_dataframe(self, data: pd.DataFrame) -> str:
        """序列化DataFrame数据"""
        # 将DataFrame转换为字典格式
        df_dict = data.to_dict('records')
        
        serialized = {
            'type': 'dataframe',
            'schema': self.schema.to_string(),
            'data': df_dict
        }
        return json.dumps(serialized, ensure_ascii=False)
    
    @classmethod
    def from_string(cls, schema_str: str) -> 'DataSerializer':
        """
        从schema字符串创建序列化器
        
        Args:
            schema_str: schema字符串
            
        Returns:
            DataSerializer: 数据序列化器
        """
        schema = SchemaValidator.parse_schema(schema_str)
        return cls(schema)
    
    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any]) -> 'DataSerializer':
        """
        从字典创建序列化器
        
        Args:
            schema_dict: schema字典
            
        Returns:
            DataSerializer: 数据序列化器
        """
        schema_type = schema_dict.get('type')
        if schema_type == 'value':
            schema = ValueSchema.from_dict(schema_dict)
        elif schema_type == 'dataframe':
            schema = DataFrameSchema.from_dict(schema_dict)
        else:
            raise ValueError(f"不支持的schema类型: {schema_type}")
        
        return cls(schema)


class DataDeserializer:
    """数据反序列化器"""
    
    def __init__(self, schema: Optional[Schema] = None):
        """
        初始化数据反序列化器
        
        Args:
            schema: 数据格式定义，如果为None则从序列化数据中获取
        """
        self.schema = schema
    
    def deserialize(self, serialized_data: str) -> Any:
        """
        反序列化字符串为数据
        
        Args:
            serialized_data: 序列化的字符串
            
        Returns:
            Any: 反序列化后的数据
        """
        try:
            # 解析JSON
            data_dict = json.loads(serialized_data)
            
            # 获取schema
            schema_str = data_dict.get('schema')
            if not schema_str:
                raise ValueError("序列化数据中缺少schema信息")
            
            # 如果未指定schema，从序列化数据中获取
            if self.schema is None:
                self.schema = SchemaValidator.parse_schema(schema_str)
            
            # 验证schema是否匹配
            if self.schema.to_string() != schema_str:
                raise ValueError("Schema不匹配")
            
            # 根据类型进行反序列化
            data_type = data_dict.get('type')
            data = data_dict.get('data')
            
            if data_type == 'value':
                return self._deserialize_value(data)
            elif data_type == 'dataframe':
                return self._deserialize_dataframe(data)
            else:
                raise ValueError(f"不支持的数据类型: {data_type}")
                
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON解析失败: {e}")
        except Exception as e:
            raise ValueError(f"反序列化失败: {e}")
    
    def _deserialize_value(self, data: Any) -> Any:
        """反序列化单值数据"""
        # 对于单值数据，直接返回即可
        return data
    
    def _deserialize_dataframe(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """反序列化DataFrame数据"""
        # 将字典列表转换为DataFrame
        df = pd.DataFrame(data)
        
        # 验证数据格式
        if not self.schema.validate(df):
            raise ValueError("反序列化后的数据格式不符合schema定义")
        
        return df
    
    @classmethod
    def from_string(cls, schema_str: str) -> 'DataDeserializer':
        """
        从schema字符串创建反序列化器
        
        Args:
            schema_str: schema字符串
            
        Returns:
            DataDeserializer: 数据反序列化器
        """
        schema = SchemaValidator.parse_schema(schema_str)
        return cls(schema)
    
    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any]) -> 'DataDeserializer':
        """
        从字典创建反序列化器
        
        Args:
            schema_dict: schema字典
            
        Returns:
            DataDeserializer: 数据反序列化器
        """
        schema_type = schema_dict.get('type')
        if schema_type == 'value':
            schema = ValueSchema.from_dict(schema_dict)
        elif schema_type == 'dataframe':
            schema = DataFrameSchema.from_dict(schema_dict)
        else:
            raise ValueError(f"不支持的schema类型: {schema_type}")
        
        return cls(schema)


class DataSerializerManager:
    """数据序列化管理器，提供便捷的序列化和反序列化功能"""
    
    @staticmethod
    def serialize_with_schema(data: Any, schema_str: str) -> str:
        """
        使用schema字符串序列化数据
        
        Args:
            data: 待序列化的数据
            schema_str: schema字符串
            
        Returns:
            str: 序列化后的字符串
        """
        serializer = DataSerializer.from_string(schema_str)
        return serializer.serialize(data)
    
    @staticmethod
    def deserialize_with_schema(serialized_data: str, schema_str: str) -> Any:
        """
        使用schema字符串反序列化数据
        
        Args:
            serialized_data: 序列化的字符串
            schema_str: schema字符串
            
        Returns:
            Any: 反序列化后的数据
        """
        deserializer = DataDeserializer.from_string(schema_str)
        return deserializer.deserialize(serialized_data)
    
    @staticmethod
    def deserialize_auto(serialized_data: str) -> Any:
        """
        自动反序列化数据（从序列化数据中获取schema）
        
        Args:
            serialized_data: 序列化的字符串
            
        Returns:
            Any: 反序列化后的数据
        """
        deserializer = DataDeserializer()
        return deserializer.deserialize(serialized_data)
    
    @staticmethod
    def validate_and_serialize(data: Any, schema_str: str) -> Tuple[str, bool]:
        """
        校验并序列化数据
        
        Args:
            data: 待序列化的数据
            schema_str: schema字符串
            
        Returns:
            Tuple[str, bool]: (序列化后的字符串, 是否校验成功)
        """
        try:
            serializer = DataSerializer.from_string(schema_str)
            serialized = serializer.serialize(data)
            return serialized, True
        except Exception:
            return "", False 