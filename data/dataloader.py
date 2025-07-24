
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import numpy as np
from .redis_connector import RedisConnector
from utils.datetime_parser import parse_datetime, parse_datetime_to_timestamp
import re


class DataLoader:
    """数据加载器：根据data_config从Redis中加载数据"""
    
    def __init__(self, redis_connector: RedisConnector):
        """
        初始化数据加载器
        
        Args:
            redis_connector: Redis连接器实例
        """
        self.redis_connector = redis_connector
    
    def load_data(self, data_config: Dict[str, Any], placeholders: Dict[str, Any], job_datetime: str) -> Dict[str, Any]:
        """
        根据data_config加载数据
        
        Args:
            data_config: 数据配置
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，格式为 %Y-%m-%d %H:%M:%S
            
        Returns:
            加载的数据字典
        """
        # 验证占位符
        self._validate_placeholders(data_config, placeholders)
        
        # 验证job_datetime格式
        try:
            datetime.strptime(job_datetime, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError(f"job_datetime格式错误，应为 %Y-%m-%d %H:%M:%S 格式，当前值: {job_datetime}")
        
        # 加载所有变量
        loaded_data = {}
        for variable_config in data_config.get("variable", []):
            variable_name = variable_config["name"]
            try:
                loaded_data[variable_name] = self._load_variable(
                    variable_config, 
                    data_config["namespace"], 
                    placeholders,
                    job_datetime
                )
                print(f"✅ 成功加载变量: {variable_name}")
            except Exception as e:
                print(f"❌ 加载变量失败: {variable_name}, 错误: {e}")
                loaded_data[variable_name] = None
        
        return loaded_data
    
    def _validate_placeholders(self, data_config: Dict[str, Any], placeholders: Dict[str, Any]):
        """验证占位符是否完整"""
        required_placeholders = {p["name"] for p in data_config.get("placeholder", [])}
        provided_placeholders = set(placeholders.keys())
        
        missing = required_placeholders - provided_placeholders
        if missing:
            raise ValueError(f"缺少必需的占位符: {missing}")
    
    def _load_variable(self, variable_config: Dict[str, Any], namespace: str, placeholders: Dict[str, Any], job_datetime: str) -> Any:
        """
        加载单个变量
        
        Args:
            variable_config: 变量配置
            namespace: 命名空间
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，格式为 %Y-%m-%d %H:%M:%S
            
        Returns:
            加载的变量数据
        """
        redis_config = variable_config["redis_config"]
        redis_type = redis_config["type"]
        
        # 使用变量自己的namespace，如果没有则使用全局namespace
        effective_namespace = variable_config.get("namespace") or namespace
        
        if redis_type in ["value", "json"]:
            return self._load_simple_variable(redis_config, effective_namespace, placeholders, job_datetime)
        elif redis_type == "timeseries":
            return self._load_timeseries(variable_config, effective_namespace, placeholders, job_datetime)
        else:
            raise ValueError(f"不支持的Redis类型: {redis_type}")
    
    def _build_redis_key(self, namespace: str, redis_type: str, prefixes: List[Dict[str, str]], field: str, placeholders: Dict[str, Any], job_datetime: str = None) -> str:
        """
        构建Redis键名
        
        Args:
            namespace: 命名空间
            redis_type: Redis类型 (value, json, timeseries)
            prefixes: 前缀配置列表
            field: 字段名
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，用于解析日期时间占位符
            
        Returns:
            Redis键名
        """
        # 替换占位符
        prefix_parts = []
        for prefix in prefixes:
            key = prefix["key"]
            value_template = prefix["value"]
            # 替换占位符 ${placeholder_name} 和日期时间模式
            value = self._replace_placeholders(value_template, placeholders, job_datetime)
            prefix_parts.append(f"{key}={value}")
        
        ##  按字典序排序前缀
        prefix_parts.sort()
        
        # 构建完整的键名
        if prefix_parts:
            redis_key = f"{namespace}::{redis_type}::{'::'.join(prefix_parts)}::{field}"
        else:
            redis_key = f"{namespace}::{redis_type}::{field}"
        
        return redis_key
    
    def _replace_placeholders(self, template: str, placeholders: Dict[str, Any], job_datetime: str = None) -> str:
        """先用placeholders替换占位符，再判断是否有日期表达式并进行日期替换"""
        if not template:
            return template
        # 1. 先识别日期表达式，避免被当作占位符处理
        date_pattern = r'\$\{(yyyyMMdd|yyyy-MM-dd|yyyyMMddHHmmss|yyyy-MM-dd HH:mm:ss)([+-]\d+[yMwHmd])?\}'
        
        # 2. 替换非日期表达式的占位符
        def replace_func(match):
            placeholder_name = match.group(1)
            # 检查是否是日期表达式
            if re.match(date_pattern, match.group(0)):
                return match.group(0)  # 保留日期表达式，稍后处理
            # 处理普通占位符
            if placeholder_name in placeholders:
                return str(placeholders[placeholder_name])
            else:
                raise ValueError(f"占位符 '{placeholder_name}' 未找到")
        
        result = re.sub(r'\$\{([^}]+)\}', replace_func, template)
        
        # 3. 处理日期表达式
        if re.search(date_pattern, result):
            # job_datetime如果是字符串，转为datetime
            base_dt = None
            if job_datetime:
                if isinstance(job_datetime, str):
                    base_dt = datetime.strptime(job_datetime, "%Y-%m-%d %H:%M:%S")
                else:
                    base_dt = job_datetime
            result = parse_datetime(result, base_datetime=base_dt)
        return result
    
    def _load_simple_variable(self, redis_config: Dict[str, Any], namespace: str, placeholders: Dict[str, Any], job_datetime: str) -> Any:
        """
        加载简单变量（variable类型）
        
        Args:
            redis_config: Redis配置
            namespace: 命名空间
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，格式为 %Y-%m-%d %H:%M:%S
            
        Returns:
            变量值
        """
        redis_type = redis_config["type"]
        field = redis_config["field"]
        prefixes = redis_config.get("prefix", [])
        
        redis_key = self._build_redis_key(namespace, redis_type, prefixes, field, placeholders, job_datetime)
        
        if redis_type == "value":
            return self.redis_connector.get_direct_variable(namespace, self._extract_key_suffix(redis_key, namespace, "value"))
        elif redis_type == "json":
            return self.redis_connector.get_json_variable(namespace, self._extract_key_suffix(redis_key, namespace, "json"))
        else:
            raise ValueError(f"简单变量不支持Redis类型: {redis_type}")
    
    def _extract_key_suffix(self, full_key: str, namespace: str, redis_type: str) -> str:
        """从完整的Redis键名中提取后缀部分"""
        prefix = f"{namespace}::{redis_type}::"
        if full_key.startswith(prefix):
            return full_key[len(prefix):]
        return full_key
    
    def _load_timeseries(self, variable_config: Dict[str, Any], namespace: str, placeholders: Dict[str, Any], job_datetime: str) -> pd.DataFrame:
        """
        加载时间序列数据
        
        Args:
            variable_config: 变量配置
            namespace: 命名空间
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，格式为 %Y-%m-%d %H:%M:%S
            
        Returns:
            DataFrame格式的时间序列数据
        """
        redis_config = variable_config["redis_config"]
        
        # 获取去重策略，默认为"none"
        drop_duplicate = redis_config.get("drop_duplicate", "none")
        
        # 获取时间序列数据
        timeseries_data = self._get_timeseries_data(redis_config, namespace, placeholders, job_datetime)
        
        if not timeseries_data:
            # 返回空DataFrame
            return pd.DataFrame()
        
        # 转换为DataFrame
        df = self._convert_timeseries_to_dataframe(timeseries_data, drop_duplicate)
        
        return df
    
    def _get_timeseries_data(self, redis_config: Dict[str, Any], namespace: str, placeholders: Dict[str, Any], job_datetime: str) -> List[Dict]:
        """
        从Redis获取时间序列数据
        
        Args:
            redis_config: Redis配置
            namespace: 命名空间
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，格式为 %Y-%m-%d %H:%M:%S
            
        Returns:
            时间序列数据列表
        """
        field = redis_config["field"]
        prefixes = redis_config.get("prefix", [])
        from_datetime = redis_config.get("from_datetime")
        to_datetime = redis_config.get("to_datetime")
        
        # 解析job_datetime为datetime对象
        job_dt = datetime.strptime(job_datetime, "%Y-%m-%d %H:%M:%S")
        from_timestamp = None
        to_timestamp = None
        
        if from_datetime:
            from_datetime_str = parse_datetime(from_datetime, base_datetime=job_dt)
            from_timestamp = parse_datetime_to_timestamp(from_datetime_str)
        if to_datetime:
            to_datetime_str = parse_datetime(to_datetime, base_datetime=job_dt)
            to_timestamp = parse_datetime_to_timestamp(to_datetime_str)
        
        redis_key = self._build_redis_key(namespace, "timeseries", prefixes, field, placeholders, job_datetime)
        series_key = self._extract_key_suffix(redis_key, namespace, "timeseries")
        
        return self.redis_connector.get_timeseries_range(
            namespace, 
            series_key, 
            start_time=from_timestamp, 
            end_time=to_timestamp
        )
    
    def _convert_timeseries_to_dataframe(self, timeseries_data: List[Dict], drop_duplicate: str = "none") -> pd.DataFrame:
        """
        将时间序列数据转换为DataFrame
        
        Args:
            timeseries_data: 时间序列数据
            drop_duplicate: 去重策略，"none"或"keep_latest"
            
        Returns:
            DataFrame
        """
        if not timeseries_data:
            return pd.DataFrame()
        
        # 展开数据
        rows = []
        for item in timeseries_data:
            timestamp = item["timestamp"]
            value_data = item["value"]
            
            # 添加所有字段
            row = {}
            for key, value in value_data.items():
                row[key] = value
            row["__timestamp__"] = timestamp
            rows.append(row)
        
        # 创建DataFrame
        df = pd.DataFrame(rows)
        
        if len(df) == 0:
            return df
        
        # 根据去重策略处理数据
        if drop_duplicate == "none":
            # 不做任何处理，容忍同一个score有多个值
            df = df.sort_values('__timestamp__').reset_index(drop=True)
            df = df.drop(columns=['__timestamp__'])
        elif drop_duplicate == "keep_latest":
            # 根据etl_time字段，筛选出最新的字段
            if 'etl_time' not in df.columns:
                raise ValueError("使用keep_latest策略时，数据中必须包含etl_time字段")
            
            # 按etl_time排序，保留最新的记录
            df = df.sort_values('etl_time', ascending=False)
            df = df.drop_duplicates(subset=['__timestamp__'], keep='first')
            df = df.sort_values('__timestamp__').reset_index(drop=True)
            df = df.drop(columns=['__timestamp__'])
        else:
            raise ValueError(f"不支持的去重策略: {drop_duplicate}")
        
        return df
