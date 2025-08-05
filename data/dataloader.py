
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import numpy as np
from .redis_connector import RedisConnector
from utils.datetime_parser import parse_datetime, parse_datetime_to_timestamp
import re
import time


def parse_job_datetime(job_datetime: str) -> datetime:
    """
    解析job_datetime，支持两种格式：
    - '%Y-%m-%d' (如: '2024-01-01')
    - '%Y-%m-%d %H:%M:%S' (如: '2024-01-01 10:30:00')
    
    Args:
        job_datetime: 日期时间字符串
        
    Returns:
        datetime: 解析后的datetime对象
        
    Raises:
        ValueError: 如果日期格式不支持
    """
    # 尝试解析为完整日期时间格式
    try:
        return datetime.strptime(job_datetime, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        pass
    
    # 尝试解析为仅日期格式
    try:
        return datetime.strptime(job_datetime, '%Y-%m-%d')
    except ValueError:
        pass
    
    # 如果两种格式都失败，抛出异常
    raise ValueError(f"不支持的日期格式: {job_datetime}。支持的格式: 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'")


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
        根据data_config加载数据（单次查询方式）
        
        Args:
            data_config: 数据配置
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，支持格式：'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            加载的数据字典
        """
        # 验证占位符
        self._validate_placeholders(data_config, placeholders)
        
        # 验证job_datetime格式
        try:
            parse_job_datetime(job_datetime)
        except ValueError as e:
            raise ValueError(f"job_datetime格式错误: {e}")
        
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
    
    def load_data_batch(self, data_config: Dict[str, Any], placeholders: Dict[str, Any], job_datetime: str) -> Dict[str, Any]:
        """
        根据data_config批量加载数据（使用mget方式）
        
        Args:
            data_config: 数据配置
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，支持格式：'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            加载的数据字典
        """
        # 验证占位符
        self._validate_placeholders(data_config, placeholders)
        
        # 验证job_datetime格式
        try:
            parse_job_datetime(job_datetime)
        except ValueError as e:
            raise ValueError(f"job_datetime格式错误: {e}")
        
        # 收集所有需要批量获取的键
        batch_keys = self._collect_batch_keys(data_config, placeholders, job_datetime)
        
        # 批量获取数据
        batch_results = self._batch_get_data(batch_keys)
        
        # 处理结果并构建返回数据
        loaded_data = {}
        for variable_config in data_config.get("variable", []):
            variable_name = variable_config["name"]
            try:
                loaded_data[variable_name] = self._process_batch_result(
                    variable_config,
                    data_config["namespace"],
                    batch_results,
                    placeholders,
                    job_datetime
                )
                print(f"✅ 成功批量加载变量: {variable_name}")
            except Exception as e:
                print(f"❌ 批量加载变量失败: {variable_name}, 错误: {e}")
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
            job_datetime: 作业日期时间，支持格式：'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
            
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
        elif redis_type == "densets":
            return self._load_densets(variable_config, effective_namespace, placeholders, job_datetime)
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
                    base_dt = parse_job_datetime(job_datetime)
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
            job_datetime: 作业日期时间，支持格式：'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
            
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
            job_datetime: 作业日期时间，支持格式：'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
            
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
            job_datetime: 作业日期时间，支持格式：'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            时间序列数据列表
        """
        field = redis_config["field"]
        prefixes = redis_config.get("prefix", [])
        from_datetime = redis_config.get("from_datetime")
        to_datetime = redis_config.get("to_datetime")
        
        # 解析job_datetime为datetime对象
        job_dt = parse_job_datetime(job_datetime)
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

    def _load_densets(self, variable_config: Dict[str, Any], namespace: str, placeholders: Dict[str, Any], job_datetime: str) -> pd.DataFrame:
        """
        加载densets数据
        
        Args:
            variable_config: 变量配置
            namespace: 命名空间
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，支持格式：'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            DataFrame格式的densets数据
        """
        redis_config = variable_config["redis_config"]
        
        # 获取去重策略，默认为"none"
        drop_duplicate = redis_config.get("drop_duplicate", "none")
        
        # 获取分隔符，默认为","
        split_char = redis_config.get("split", ",")
        
        # 获取schema字符串
        schema_str = redis_config.get("schema")
        
        # 获取densets数据
        densets_data = self._get_densets_data(redis_config, namespace, placeholders, job_datetime)
        
        if not densets_data:
            # 返回空DataFrame
            return pd.DataFrame()
        
        # 转换为DataFrame
        df = self._convert_densets_to_dataframe(densets_data, drop_duplicate, split_char, schema_str)
        
        return df
    
    def _get_densets_data(self, redis_config: Dict[str, Any], namespace: str, placeholders: Dict[str, Any], job_datetime: str) -> List[Dict]:
        """
        从Redis获取densets数据
        
        Args:
            redis_config: Redis配置
            namespace: 命名空间
            placeholders: 占位符值映射
            job_datetime: 作业日期时间，支持格式：'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
            
        Returns:
            densets数据列表
        """
        field = redis_config["field"]
        prefixes = redis_config.get("prefix", [])
        from_datetime = redis_config.get("from_datetime")
        to_datetime = redis_config.get("to_datetime")
        
        # 解析job_datetime为datetime对象
        job_dt = parse_job_datetime(job_datetime)
        from_timestamp = None
        to_timestamp = None
        
        if from_datetime:
            from_datetime_str = parse_datetime(from_datetime, base_datetime=job_dt)
            from_timestamp = parse_datetime_to_timestamp(from_datetime_str)
        if to_datetime:
            to_datetime_str = parse_datetime(to_datetime, base_datetime=job_dt)
            to_timestamp = parse_datetime_to_timestamp(to_datetime_str)
        
        redis_key = self._build_redis_key(namespace, "densets", prefixes, field, placeholders, job_datetime)
        series_key = self._extract_key_suffix(redis_key, namespace, "densets")
        
        return self.redis_connector.get_densets_range(
            namespace, 
            series_key, 
            start_time=from_timestamp, 
            end_time=to_timestamp
        )
    
    def _convert_densets_to_dataframe(self, densets_data: List[Dict], drop_duplicate: str = "none", split_char: str = ",", schema_str: str = None) -> pd.DataFrame:
        """
        将densets数据转换为DataFrame
        
        Args:
            densets_data: densets数据
            drop_duplicate: 去重策略，"none"或"keep_latest"
            split_char: 分隔符
            schema_str: schema字符串，如 "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string"
            
        Returns:
            DataFrame
        """
        if not densets_data:
            return pd.DataFrame()
        
        # 解析schema字符串
        schema_columns = []
        if schema_str:
            schema_columns = self._parse_schema_string(schema_str)
        
        # 展开数据
        rows = []
        for item in densets_data:
            timestamp = item["timestamp"]
            value_str = item["value"]
            
            # 按分隔符分割字符串
            values = value_str.split(split_char)
            
            # 验证值的数量
            if schema_columns and len(values) < len(schema_columns):
                raise ValueError(f"数据值数量不足: 期望至少 {len(schema_columns)} 个值，实际只有 {len(values)} 个值。数据: {value_str}")
            
            # 创建行数据
            if schema_columns:
                # 如果有schema，创建有意义的列并进行类型验证
                row = {"__timestamp__": timestamp}
                for i, (col_name, col_type) in enumerate(schema_columns):
                    if i < len(values):
                        # 类型转换和验证
                        converted_value = self._convert_value_type(values[i], col_type)
                        row[col_name] = converted_value
                rows.append(row)
            else:
                # 否则保持原始格式
                row = {
                    "__timestamp__": timestamp,
                    "__values__": values
                }
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
            # 对于 densets，如果有 etl_time 字段，按 etl_time 排序；否则按时间戳排序
            if 'etl_time' in df.columns:
                # 按 etl_time 排序，保留最新的记录
                df = df.sort_values('etl_time', ascending=False)
                df = df.drop_duplicates(subset=['__timestamp__'], keep='first')
                df = df.sort_values('__timestamp__').reset_index(drop=True)
                df = df.drop(columns=['__timestamp__'])
            else:
                # 没有 etl_time 字段，按时间戳排序（Redis 中后添加的数据会覆盖先添加的）
                df = df.sort_values('__timestamp__', ascending=False)
                df = df.drop_duplicates(subset=['__timestamp__'], keep='first')
                df = df.sort_values('__timestamp__').reset_index(drop=True)
                df = df.drop(columns=['__timestamp__'])
        else:
            raise ValueError(f"不支持的去重策略: {drop_duplicate}")
        
        return df
    
    def _parse_schema_string(self, schema_str: str) -> List[tuple]:
        """
        解析schema字符串
        
        Args:
            schema_str: schema字符串，如 "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string"
            
        Returns:
            列名和类型的元组列表
        """
        if not schema_str.strip():
            return []
        
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
            
            # 验证类型
            valid_types = {'string', 'int', 'double', 'boolean'}
            if col_type not in valid_types:
                raise ValueError(f"不支持的类型: {col_type}，支持的类型: {valid_types}")
            
            columns.append((name, col_type))
        
        return columns
    
    def _convert_value_type(self, value: str, target_type: str) -> Any:
        """
        将字符串值转换为指定类型
        
        Args:
            value: 字符串值
            target_type: 目标类型
            
        Returns:
            转换后的值
        """
        if target_type == 'string':
            return value
        elif target_type == 'int':
            try:
                return int(value)
            except ValueError:
                raise ValueError(f"无法将值 '{value}' 转换为 int 类型")
        elif target_type == 'double':
            try:
                return float(value)
            except ValueError:
                raise ValueError(f"无法将值 '{value}' 转换为 double 类型")
        elif target_type == 'boolean':
            value_lower = value.lower()
            if value_lower in {'true', '1', 'yes', 'on'}:
                return True
            elif value_lower in {'false', '0', 'no', 'off'}:
                return False
            else:
                raise ValueError(f"无法将值 '{value}' 转换为 boolean 类型")
        else:
            raise ValueError(f"不支持的类型: {target_type}")

    def _collect_batch_keys(self, data_config: Dict[str, Any], placeholders: Dict[str, Any], job_datetime: str) -> Dict[str, Dict[str, Any]]:
        """
        收集所有需要批量获取的键
        
        Returns:
            键名到配置的映射
        """
        batch_keys = {}
        
        for variable_config in data_config.get("variable", []):
            variable_name = variable_config["name"]
            redis_config = variable_config["redis_config"]
            redis_type = redis_config["type"]
            
            # 只处理简单变量类型（value和json），时间序列和densets需要单独处理
            if redis_type in ["value", "json"]:
                effective_namespace = variable_config.get("namespace") or data_config["namespace"]
                field = redis_config["field"]
                prefixes = redis_config.get("prefix", [])
                
                redis_key = self._build_redis_key(effective_namespace, redis_type, prefixes, field, placeholders, job_datetime)
                key_suffix = self._extract_key_suffix(redis_key, effective_namespace, redis_type)
                
                batch_keys[redis_key] = {
                    "variable_name": variable_name,
                    "redis_type": redis_type,
                    "namespace": effective_namespace,
                    "key_suffix": key_suffix,
                    "variable_config": variable_config
                }
        
        return batch_keys
    
    def _batch_get_data(self, batch_keys: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        批量获取数据
        
        Args:
            batch_keys: 键名到配置的映射
            
        Returns:
            键名到数据的映射
        """
        if not batch_keys:
            return {}
        
        # 按类型分组
        value_keys = []
        json_keys = []
        
        for redis_key, config in batch_keys.items():
            if config["redis_type"] == "value":
                value_keys.append((redis_key, config))
            elif config["redis_type"] == "json":
                json_keys.append((redis_key, config))
        
        batch_results = {}
        
        # 批量获取value类型数据
        if value_keys:
            value_namespaces = {}
            for redis_key, config in value_keys:
                namespace = config["namespace"]
                if namespace not in value_namespaces:
                    value_namespaces[namespace] = []
                value_namespaces[namespace].append((redis_key, config))
            
            for namespace, keys_configs in value_namespaces.items():
                key_suffixes = [config["key_suffix"] for _, config in keys_configs]
                results = self.redis_connector.redis_client.mget([f"{namespace}::value::{suffix}" for suffix in key_suffixes])
                
                for i, (redis_key, config) in enumerate(keys_configs):
                    batch_results[redis_key] = results[i]
        
        # 批量获取json类型数据
        if json_keys:
            json_namespaces = {}
            for redis_key, config in json_keys:
                namespace = config["namespace"]
                if namespace not in json_namespaces:
                    json_namespaces[namespace] = []
                json_namespaces[namespace].append((redis_key, config))
            
            for namespace, keys_configs in json_namespaces.items():
                key_suffixes = [config["key_suffix"] for _, config in keys_configs]
                results = self.redis_connector.redis_client.mget([f"{namespace}::json::{suffix}" for suffix in key_suffixes])
                
                for i, (redis_key, config) in enumerate(keys_configs):
                    batch_results[redis_key] = results[i]
        
        return batch_results
    
    def _process_batch_result(self, variable_config: Dict[str, Any], namespace: str, batch_results: Dict[str, Any], placeholders: Dict[str, Any], job_datetime: str) -> Any:
        """
        处理批量获取的结果
        
        Args:
            variable_config: 变量配置
            namespace: 命名空间
            batch_results: 批量获取的结果
            placeholders: 占位符值映射
            job_datetime: 作业日期时间
            
        Returns:
            处理后的变量数据
        """
        redis_config = variable_config["redis_config"]
        redis_type = redis_config["type"]
        
        effective_namespace = variable_config.get("namespace") or namespace
        
        if redis_type in ["value", "json"]:
            field = redis_config["field"]
            prefixes = redis_config.get("prefix", [])
            redis_key = self._build_redis_key(effective_namespace, redis_type, prefixes, field, placeholders, job_datetime)
            
            if redis_key not in batch_results or batch_results[redis_key] is None:
                raise KeyError(f"变量 '{variable_config['name']}' 在Redis中不存在")
            
            if redis_type == "value":
                return batch_results[redis_key]
            elif redis_type == "json":
                import json
                return json.loads(batch_results[redis_key])
        elif redis_type == "timeseries":
            # 时间序列仍然需要单独处理
            return self._load_timeseries(variable_config, effective_namespace, placeholders, job_datetime)
        elif redis_type == "densets":
            # densets 仍然需要单独处理
            return self._load_densets(variable_config, effective_namespace, placeholders, job_datetime)
        else:
            raise ValueError(f"不支持的Redis类型: {redis_type}")
