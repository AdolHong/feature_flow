"""
数据校验模块
提供数据格式校验功能
"""

from typing import Any, Dict, List, Union
from .schema import Schema, ValueSchema, DataFrameSchema


class SchemaValidator:
    """Schema校验器"""
    
    @staticmethod
    def validate_schema_string(schema_str: str) -> bool:
        """
        验证schema字符串格式是否正确
        
        Args:
            schema_str: schema字符串
            
        Returns:
            bool: 格式是否正确
        """
        try:
            if not schema_str or not schema_str.strip():
                return False
            
            # 尝试解析为ValueSchema
            try:
                ValueSchema.from_string(schema_str)
                return True
            except ValueError:
                pass
            
            # 尝试解析为DataFrameSchema
            try:
                DataFrameSchema.from_string(schema_str)
                return True
            except ValueError:
                pass
            
            return False
        except Exception:
            return False
    
    @staticmethod
    def detect_schema_type(schema_str: str) -> str:
        """
        检测schema字符串的类型
        
        Args:
            schema_str: schema字符串
            
        Returns:
            str: 'value' 或 'dataframe'
        """
        if not schema_str or not schema_str.strip():
            raise ValueError("Schema字符串不能为空")
        
        # 检查是否为DataFrame schema (包含冒号)
        if ':' in schema_str:
            return 'dataframe'
        else:
            return 'value'
    
    @staticmethod
    def parse_schema(schema_str: str) -> Schema:
        """
        解析schema字符串为Schema对象
        
        Args:
            schema_str: schema字符串
            
        Returns:
            Schema: 解析后的Schema对象
        """
        if not schema_str or not schema_str.strip():
            raise ValueError("Schema字符串不能为空")
        
        schema_type = SchemaValidator.detect_schema_type(schema_str)
        
        if schema_type == 'value':
            return ValueSchema.from_string(schema_str)
        else:
            return DataFrameSchema.from_string(schema_str)


class DataValidator:
    """数据校验器"""
    
    def __init__(self, schema: Schema):
        """
        初始化数据校验器
        
        Args:
            schema: 数据格式定义
        """
        self.schema = schema
    
    def validate(self, data: Any) -> bool:
        """
        校验数据是否符合schema
        
        Args:
            data: 待校验的数据
            
        Returns:
            bool: 是否符合schema
        """
        try:
            return self.schema.validate(data)
        except Exception:
            return False
    
    @classmethod
    def from_string(cls, schema_str: str) -> 'DataValidator':
        """
        从schema字符串创建校验器
        
        Args:
            schema_str: schema字符串
            
        Returns:
            DataValidator: 数据校验器
        """
        schema = SchemaValidator.parse_schema(schema_str)
        return cls(schema)
    
    @classmethod
    def from_dict(cls, schema_dict: Dict[str, Any]) -> 'DataValidator':
        """
        从字典创建校验器
        
        Args:
            schema_dict: schema字典
            
        Returns:
            DataValidator: 数据校验器
        """
        schema_type = schema_dict.get('type')
        if schema_type == 'value':
            schema = ValueSchema.from_dict(schema_dict)
        elif schema_type == 'dataframe':
            schema = DataFrameSchema.from_dict(schema_dict)
        else:
            raise ValueError(f"不支持的schema类型: {schema_type}")
        
        return cls(schema)
    
    def get_schema_string(self) -> str:
        """
        获取schema字符串表示
        
        Returns:
            str: schema字符串
        """
        return self.schema.to_string()
    
    def get_schema_dict(self) -> Dict[str, Any]:
        """
        获取schema字典表示
        
        Returns:
            Dict[str, Any]: schema字典
        """
        return self.schema.to_dict()


class MultiDataValidator:
    """多数据校验器，用于校验多个数据源"""
    
    def __init__(self, validators: Dict[str, DataValidator]):
        """
        初始化多数据校验器
        
        Args:
            validators: 校验器字典，key为数据源名称，value为对应的校验器
        """
        self.validators = validators
    
    def validate_all(self, data_dict: Dict[str, Any]) -> Dict[str, bool]:
        """
        校验所有数据
        
        Args:
            data_dict: 数据字典，key为数据源名称，value为对应的数据
            
        Returns:
            Dict[str, bool]: 校验结果字典
        """
        results = {}
        for name, validator in self.validators.items():
            data = data_dict.get(name)
            results[name] = validator.validate(data)
        return results
    
    def validate_single(self, name: str, data: Any) -> bool:
        """
        校验单个数据源
        
        Args:
            name: 数据源名称
            data: 数据
            
        Returns:
            bool: 校验结果
        """
        if name not in self.validators:
            raise ValueError(f"未找到数据源: {name}")
        
        return self.validators[name].validate(data)
    
    def get_invalid_data(self, data_dict: Dict[str, Any]) -> List[str]:
        """
        获取校验失败的数据源名称列表
        
        Args:
            data_dict: 数据字典
            
        Returns:
            List[str]: 校验失败的数据源名称列表
        """
        results = self.validate_all(data_dict)
        return [name for name, is_valid in results.items() if not is_valid]
    
    def get_valid_data(self, data_dict: Dict[str, Any]) -> List[str]:
        """
        获取校验成功的数据源名称列表
        
        Args:
            data_dict: 数据字典
            
        Returns:
            List[str]: 校验成功的数据源名称列表
        """
        results = self.validate_all(data_dict)
        return [name for name, is_valid in results.items() if is_valid] 