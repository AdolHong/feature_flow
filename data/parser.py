import pandas as pd
import json
import numpy as np
from typing import Any, Dict, Union, Optional
from datetime import datetime


class Parser:
    """
    通用解析器
    用于解析DataFrame行数据或字典数据，将每一列/字段转换为Python类型
    """
    
    def __init__(self):
        self.type_mapping = {
            'int64': int,
            'int32': int,
            'float64': float,
            'float32': float,
            'object': str,
            'bool': bool,
            'datetime64[ns]': datetime
        }
    
    def parse_row(self, row: pd.Series) -> Dict[str, Any]:
        """
        解析DataFrame的一行数据
        
        Args:
            row: pandas Series对象，代表一行数据
            
        Returns:
            Dict[str, Any]: 解析后的变量字典
        """
        result = {}
        
        for column_name, value in row.items():
            # 跳过索引列
            if column_name == 'Unnamed: 0':
                continue
                
            parsed_value = self._parse_value(value, column_name)
            result[column_name] = parsed_value
            
        return result
    
    def parse_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        解析字典数据
        
        Args:
            data: 字典对象，key为字段名，value为值
            
        Returns:
            Dict[str, Any]: 解析后的变量字典
        """
        result = {}
        
        for field_name, value in data.items():
            parsed_value = self._parse_value(value, field_name)
            result[field_name] = parsed_value
            
        return result
    
    def _parse_value(self, value: Any, column_name: str) -> Any:
        """
        解析单个值
        
        Args:
            value: 原始值
            column_name: 列名
            
        Returns:
            Any: 解析后的值
        """
        # 处理NaN值
        if pd.isna(value):
            return None
            
        # 处理numpy类型
        if isinstance(value, np.integer):
            return int(value)
        elif isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif isinstance(value, pd.Timestamp):
            return str(value)
        elif isinstance(value, np.datetime64):
            return str(value)
        
        # 处理字符串类型
        if isinstance(value, str):
            return self._parse_string_value(value, column_name)
        
        # 处理其他类型
        return value
    
    def _parse_string_value(self, value: str, column_name: str) -> Any:
        """
        解析字符串值
        
        Args:
            value: 字符串值
            column_name: 列名
            
        Returns:
            Any: 解析后的值
        """
        # 尝试JSON解析
        try:
            parsed_json = json.loads(value)
            
            # 检查是否包含type和records字段
            if isinstance(parsed_json, dict) and 'type' in parsed_json and 'records' in parsed_json:
                if parsed_json['type'] == 'dataframe':
                    return self._parse_dataframe_json(parsed_json)
            
            return parsed_json
            
        except (json.JSONDecodeError, TypeError):
            # 如果不是JSON，尝试其他类型转换
            return self._try_type_conversion(value)
    
    def _parse_dataframe_json(self, json_obj: Dict[str, Any]) -> Union[pd.DataFrame, Dict[str, Any]]:
        """
        解析包含DataFrame信息的JSON对象
        
        Args:
            json_obj: 包含type和records字段的JSON对象
            
        Returns:
            Union[pd.DataFrame, Dict[str, Any]]: 解析后的DataFrame或原始JSON对象
        """
        try:
            records = json.loads(json_obj['records'])
            
            # 检查records是否为字典格式（列名到数组的映射）
            if isinstance(records, dict):
                # 检查所有数组长度是否一致
                arrays = list(records.values())
                if not arrays:
                    return json_obj
                
                # 检查数组长度是否一致
                array_lengths = [len(arr) if isinstance(arr, list) else 0 for arr in arrays]
                unique_lengths = set(array_lengths)
                
                # 如果数组长度不一致，返回原始JSON对象
                if len(unique_lengths) > 1:
                    return json_obj
                
                # 如果所有数组长度一致，转换为DataFrame
                df = pd.DataFrame(records)
                
                # 检查是否有额外的字段需要作为列添加
                extra_fields = {k: v for k, v in json_obj.items() if k not in ['type', 'records']}
                if extra_fields:
                    for column, value in extra_fields.items():
                        df[column] = value
                
                return df
            else:
                # 如果不是字典格式，返回原始JSON对象
                return json_obj
                
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            # 如果解析失败，返回原始JSON对象
            print(f"解析DataFrame JSON失败: {e}")
            return json_obj
    
    def _try_type_conversion(self, value: str) -> Any:
        """
        尝试将字符串转换为其他类型
        
        Args:
            value: 字符串值
            
        Returns:
            Any: 转换后的值
        """
        # 尝试转换为整数
        try:
            if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                return int(value)
        except:
            pass
        
        # 尝试转换为浮点数
        try:
            float_val = float(value)
            # 检查是否为整数
            if float_val.is_integer():
                return int(float_val)
            return float_val
        except:
            pass
        
        # 尝试转换为布尔值
        if value.lower() in ['true', 'false', '1', '0', 'yes', 'no']:
            return value.lower() in ['true', '1', 'yes']
        
        # # 尝试转换为日期时间
        # try:
        #     # 常见的日期时间格式
        #     for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
        #         try:
        #             return datetime.strptime(value, fmt)
        #         except:
        #             continue
        # except:
        #     pass
        
        # 如果都失败，返回原始字符串
        return value
    
    def parse_dataframe(self, df: pd.DataFrame) -> list:
        """
        解析整个DataFrame
        
        Args:
            df: pandas DataFrame对象
            
        Returns:
            list: 解析后的行数据列表
        """
        results = []
        for index, row in df.iterrows():
            parsed_row = self.parse_row(row)
            results.append(parsed_row)
        return results


def parse_excel_file(file_path: str) -> list:
    """
    解析Excel文件的便捷函数
    
    Args:
        file_path: Excel文件路径
        
    Returns:
        list: 解析后的数据列表
    """
    df = pd.read_excel(file_path)
    parser = Parser()
    return parser.parse_dataframe(df)


def parse_excel_row(row: pd.Series) -> Dict[str, Any]:
    """
    解析单行Excel数据的便捷函数
    
    Args:
        row: pandas Series对象
        
    Returns:
        Dict[str, Any]: 解析后的变量字典
    """
    parser = Parser()
    return parser.parse_row(row)


def parse_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    解析字典数据的便捷函数
    
    Args:
        data: 字典对象，key为字段名，value为值
        
    Returns:
        Dict[str, Any]: 解析后的变量字典
    """
    parser = Parser()
    return parser.parse_dict(data) 