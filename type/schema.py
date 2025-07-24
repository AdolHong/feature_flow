"""
数据格式定义模块
定义数据校验的schema格式，支持value和dataframe两种数据类型
"""

import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass


# 类型别名映射
TYPE_ALIASES = {
    # 字符串类型
    'str': 'string',
    'string': 'string',
    
    # 整数类型
    'int': 'int',
    
    # 浮点数类型
    'float': 'double',
    'double': 'double',
    
    # 布尔类型
    'bool': 'boolean',
    'boolean': 'boolean'
}

# 标准类型集合
STANDARD_TYPES = {'string', 'int', 'double', 'boolean'}


def normalize_type(type_name: str) -> str:
    """将类型名称标准化"""
    normalized = TYPE_ALIASES.get(type_name.lower())
    if normalized is None:
        raise ValueError(f"不支持的类型: {type_name}，支持的类型: {list(TYPE_ALIASES.keys())}")
    return normalized


@dataclass
class ColumnSchema:
    """DataFrame列定义"""
    name: str
    type: str  # 支持 str/string, int, float/double, bool/boolean
    
    def __post_init__(self):
        """验证列类型并标准化"""
        self.type = normalize_type(self.type)


class Schema(ABC):
    """Schema基类"""
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """验证数据是否符合schema"""
        pass
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，用于序列化"""
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Schema':
        """从字典格式创建Schema"""
        pass
    
    @abstractmethod
    def to_string(self) -> str:
        """转换为字符串格式，用于前端使用"""
        pass
    
    @classmethod
    @abstractmethod
    def from_string(cls, schema_str: str) -> 'Schema':
        """从字符串格式创建Schema"""
        pass


class ValueSchema(Schema):
    """单值数据格式定义"""
    
    def __init__(self, value_type: str):
        """
        初始化ValueSchema
        
        Args:
            value_type: 值类型，支持 int, double, string, boolean, dict, list, None
        """
        valid_types = {'int', 'double', 'string', 'boolean', 'dict', 'list', 'None'}
        if value_type not in valid_types:
            raise ValueError(f"不支持的值类型: {value_type}，支持的类型: {valid_types}")
        
        self.value_type = value_type
    
    def validate(self, data: Any) -> bool:
        """验证数据是否符合schema"""
        if self.value_type == 'None':
            return data is None
        elif self.value_type == 'int':
            return isinstance(data, int)
        elif self.value_type == 'double':
            return isinstance(data, (int, float))
        elif self.value_type == 'string':
            return isinstance(data, str)
        elif self.value_type == 'boolean':
            return isinstance(data, bool)
        elif self.value_type == 'dict':
            return isinstance(data, dict)
        elif self.value_type == 'list':
            return isinstance(data, list)
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'type': 'value',
            'value_type': self.value_type
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ValueSchema':
        """从字典格式创建ValueSchema"""
        if data.get('type') != 'value':
            raise ValueError("字典格式不是ValueSchema")
        return cls(data['value_type'])
    
    def to_string(self) -> str:
        """转换为字符串格式"""
        return self.value_type
    
    @classmethod
    def from_string(cls, schema_str: str) -> 'ValueSchema':
        """从字符串格式创建ValueSchema"""
        return cls(schema_str.strip())


class DataFrameSchema(Schema):
    """DataFrame数据格式定义"""
    
    def __init__(self, columns: List[ColumnSchema]):
        """
        初始化DataFrameSchema
        
        Args:
            columns: 列定义列表
        """
        self.columns = columns
        self._column_dict = {col.name: col for col in columns}
    
    def validate(self, data: Any) -> bool:
        """验证数据是否符合schema"""
        try:
            import pandas as pd
            if not isinstance(data, pd.DataFrame):
                return False
            
            # 检查列名
            df_columns = set(data.columns)
            schema_columns = set(self._column_dict.keys())
            
            if df_columns != schema_columns:
                return False
            
            # 检查数据类型
            for col_name, col_schema in self._column_dict.items():
                if col_name not in data.columns:
                    return False
                
                # 获取列的数据类型
                col_data = data[col_name]
                if not self._validate_column_type(col_data, col_schema.type):
                    return False
            
            return True
        except ImportError:
            # 如果没有pandas，进行基本检查
            if not isinstance(data, dict) and not hasattr(data, 'columns'):
                return False
            return True
    
    def _validate_column_type(self, col_data, expected_type: str) -> bool:
        """验证列的数据类型"""
        if expected_type == 'string':
            return col_data.dtype == 'object' or col_data.dtype.name == 'string'
        elif expected_type == 'int':
            return col_data.dtype in ['int64', 'int32', 'int16', 'int8']
        elif expected_type == 'double':
            return col_data.dtype in ['float64', 'float32', 'float16']
        elif expected_type == 'boolean':
            return col_data.dtype == 'bool'
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'type': 'dataframe',
            'columns': [{'name': col.name, 'type': col.type} for col in self.columns]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataFrameSchema':
        """从字典格式创建DataFrameSchema"""
        if data.get('type') != 'dataframe':
            raise ValueError("字典格式不是DataFrameSchema")
        
        columns = [ColumnSchema(col['name'], col['type']) for col in data['columns']]
        return cls(columns)
    
    def to_string(self) -> str:
        """转换为字符串格式，如: "ds:string,is_activate:int,predicted:double" """
        return ','.join([f"{col.name}:{col.type}" for col in self.columns])
    
    @classmethod
    def from_string(cls, schema_str: str) -> 'DataFrameSchema':
        """从字符串格式创建DataFrameSchema，如: "ds:string,is_activate:int,predicted:double" """
        if not schema_str.strip():
            raise ValueError("Schema字符串不能为空")
        
        columns = []
        for col_def in schema_str.split(','):
            col_def = col_def.strip()
            if ':' not in col_def:
                raise ValueError(f"列定义格式错误: {col_def}，应为 'name:type' 格式")
            
            name, col_type = col_def.split(':', 1)
            name = name.strip()
            col_type = col_type.strip()
            
            if not name:
                raise ValueError(f"列名不能为空: {col_def}")
            
            columns.append(ColumnSchema(name, col_type))
        
        return cls(columns) 