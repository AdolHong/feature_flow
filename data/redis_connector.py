import redis
import json
from typing import Dict, List, Optional, Any, Union

class RedisConnector:
    """Redis连接器"""
    
    def __init__(self, host: str = 'localhost', port: int = 6379, password: Optional[str] = None, db: int = 0, use_json_module: bool = False):
        """
        初始化Redis连接
        
        Args:
            host: Redis服务器地址
            port: Redis端口
            password: Redis密码
            db: 数据库编号
            use_json_module: 是否使用RedisJSON模块（需要安装RedisJSON）
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.use_json_module = use_json_module
        
        # 创建Redis连接
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=True
        )
        
        # 测试连接
        try:
            self.redis_client.ping()
            print(f"✅ Redis连接成功: {host}:{port}")
            
            # 如果启用JSON模块，测试是否可用
            if use_json_module:
                try:
                    # 测试JSON模块是否可用
                    self.redis_client.execute_command('JSON.SET', 'test_key', '.', '{}')
                    self.redis_client.delete('test_key')
                    print("✅ RedisJSON模块可用")
                except redis.ResponseError:
                    print("⚠️ RedisJSON模块不可用，回退到字符串模式")
                    self.use_json_module = False
                    
        except redis.ConnectionError as e:
            print(f"❌ Redis连接失败: {e}")
            raise

    # ========== JSON 变量存储相关方法 ==========
    def store_json_variable(self, namespace: str, key: str, json_values: Dict[str, Any], ttl: int = 432000):
        """
        存储JSON变量数据
        
        Args:
            namespace: 命名空间
            key: 键名
            json_values: JSON数据
            ttl: 生存时间（秒），默认5天（432000秒）
        """
        redis_key = f"{namespace}::json::{key}"
        
        if self.use_json_module:
            # 使用RedisJSON模块存储
            self.redis_client.execute_command('JSON.SET', redis_key, '.', json.dumps(json_values))
            self.redis_client.expire(redis_key, ttl)
            print(f"✅ JSON变量已存储(JSON模块): {redis_key}")
        else:
            # 使用字符串存储
            self.redis_client.set(redis_key, json.dumps(json_values), ex=ttl)
            print(f"✅ JSON变量已存储(字符串): {redis_key}")
    
    def get_json_variable(self, namespace: str, key: str) -> Any:
        """
        获取JSON变量数据
        
        Args:
            namespace: 命名空间
            key: 键名
            
        Returns:
            JSON数据
        """
        redis_key = f"{namespace}::json::{key}"
        
        if self.use_json_module:
            # 使用RedisJSON模块获取
            try:
                data_str = self.redis_client.execute_command('JSON.GET', redis_key, '.')
                if data_str is None:
                    raise KeyError(f"JSON变量 '{redis_key}' 不存在")
                return json.loads(data_str)
            except redis.ResponseError:
                raise KeyError(f"JSON变量 '{redis_key}' 不存在")
        else:
            # 使用字符串获取
            data_str = self.redis_client.get(redis_key)
            if not data_str:
                raise KeyError(f"JSON变量 '{redis_key}' 不存在")
            return json.loads(data_str)
    
    def get_json_field(self, namespace: str, key: str, field_path: str) -> Any:
        """
        获取JSON变量的特定字段（仅在使用JSON模块时有效）
        
        Args:
            namespace: 命名空间
            key: 键名
            field_path: 字段路径，如 '.name' 或 '.preferences.theme'
            
        Returns:
            字段值
        """
        if not self.use_json_module:
            # 回退到完整获取
            data = self.get_json_variable(namespace, key)
            # 简单的字段路径解析
            if field_path.startswith('.'):
                field_path = field_path[1:]
            
            current = data
            for field in field_path.split('.'):
                if isinstance(current, dict) and field in current:
                    current = current[field]
                else:
                    raise KeyError(f"字段 '{field_path}' 不存在")
            return current
        
        redis_key = f"{namespace}::json::{key}"
        try:
            result = self.redis_client.execute_command('JSON.GET', redis_key, field_path)
            if result is None:
                raise KeyError(f"字段 '{field_path}' 不存在")
            return json.loads(result)
        except redis.ResponseError:
            raise KeyError(f"字段 '{field_path}' 不存在")
    
    def update_json_field(self, namespace: str, key: str, field_path: str, value: Any):
        """
        更新JSON变量的特定字段（仅在使用JSON模块时有效）
        
        Args:
            namespace: 命名空间
            key: 键名
            field_path: 字段路径，如 '.name' 或 '.preferences.theme'
            value: 新值
        """
        if not self.use_json_module:
            # 回退到完整更新
            data = self.get_json_variable(namespace, key)
            # 简单的字段路径设置
            if field_path.startswith('.'):
                field_path = field_path[1:]
            
            current = data
            fields = field_path.split('.')
            for field in fields[:-1]:
                if field not in current:
                    current[field] = {}
                current = current[field]
            current[fields[-1]] = value
            
            # 重新存储整个对象
            self.store_json_variable(namespace, key, data)
            return
        
        redis_key = f"{namespace}::json::{key}"
        try:
            self.redis_client.execute_command('JSON.SET', redis_key, field_path, json.dumps(value))
            print(f"✅ JSON字段已更新: {redis_key}{field_path}")
        except redis.ResponseError as e:
            raise ValueError(f"无法更新字段 '{field_path}': {e}")

    # ========== direct 变量存储相关方法 ==========
    def store_direct_variable(self, namespace: str, key: str, value: Union[str, int, float, bool], ttl: int = 432000):
        """
        存储直接变量
        
        Args:
            namespace: 命名空间
            key: 键名
            value: 变量值
            ttl: 生存时间（秒），默认5天（432000秒）
        """
        redis_key = f"{namespace}::value::{key}"
        
        # 如果是复杂类型，转换为JSON
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        
        self.redis_client.set(redis_key, str(value), ex=ttl)
    
    def get_direct_variable(self, namespace: str, key: str) -> Any:
        """
        获取直接变量
        
        Args:
            namespace: 命名空间
            key: 键名
            
        Returns:
            变量值
        """
        redis_key = f"{namespace}::value::{key}"
        value = self.redis_client.get(redis_key)
        if value is None:
            raise KeyError(f"直接变量 '{redis_key}' 不存在")
        
        if isinstance(value, (str, int, float, bool)):
            return value
        else:
            raise ValueError(f"直接变量 '{redis_key}' 的值不是字符串、整数、浮点数或布尔值")
    

    # ========== 时间序列存储相关方法 ==========
    
    def add_timeseries_point(self, namespace: str, series_key: str, timestamp: float, value: Any, ttl: int = 432000):
        """
        添加时间序列数据点
        
        Args:
            namespace: 命名空间
            series_key: 时间序列键名
            timestamp: 时间戳（Unix时间戳）
            value: 数据值（将被序列化为JSON）
            ttl: 生存时间（秒），默认5天
        """
        redis_key = f"{namespace}::timeseries::{series_key}"
        
        # 将值序列化为JSON字符串
        if isinstance(value, (dict, list)):
            serialized_value = json.dumps(value)
        else:
            serialized_value = json.dumps({"value": value})
        
        # 直接添加数据，不删除相同时间戳的数据
        self.redis_client.zadd(redis_key, {serialized_value: timestamp})
        
        # 设置TTL
        self.redis_client.expire(redis_key, ttl)
        
        print(f"✅ 时间序列数据点已添加: {redis_key} @ {timestamp}")

    # ========== densets 存储相关方法 ==========
    
    def add_densets_point(self, namespace: str, series_key: str, timestamp: float, value: Any, schema: str = None, ttl: int = 432000):
        """
        添加densets数据点
        
        Args:
            namespace: 命名空间
            series_key: densets键名
            timestamp: 时间戳（Unix时间戳）
            value: 数据值（将被序列化为分隔符分隔的字符串）
            schema: 数据schema，格式如 "col1:string,col2:int,col3:double"，用于保证字段顺序
            ttl: 生存时间（秒），默认5天
        """
        redis_key = f"{namespace}::densets::{series_key}"
        
        # 将值序列化为分隔符分隔的字符串
        if isinstance(value, dict):
            if schema:
                # 根据schema顺序序列化
                schema_columns = self._parse_schema_string(schema)
                serialized_values = []
                for col_name, col_type in schema_columns:
                    if col_name in value:
                        serialized_values.append(str(value[col_name]))
                    else:
                        # 如果字段不存在，使用空字符串
                        serialized_values.append("")
                serialized_value = ",".join(serialized_values)
            else:
                # 如果没有schema，按字典键的顺序（不保证顺序）
                serialized_value = ",".join(str(v) for v in value.values())
        elif isinstance(value, list):
            serialized_value = ",".join(str(v) for v in value)
        else:
            serialized_value = str(value)
        
        # 直接添加数据，不删除相同时间戳的数据
        self.redis_client.zadd(redis_key, {serialized_value: timestamp})
        
        # 设置TTL
        self.redis_client.expire(redis_key, ttl)
        
        print(f"✅ densets数据点已添加: {redis_key} @ {timestamp}")
        if schema:
            print(f"   使用schema: {schema}")
    
    def _parse_schema_string(self, schema_str: str) -> List[tuple]:
        """
        解析schema字符串，返回列名和类型的元组列表
        
        Args:
            schema_str: schema字符串，格式如 "col1:string,col2:int,col3:double"
            
        Returns:
            列名和类型的元组列表
        """
        if not schema_str:
            return []
        
        columns = []
        for col_def in schema_str.split(','):
            col_def = col_def.strip()
            if ':' in col_def:
                col_name, col_type = col_def.split(':', 1)
                columns.append((col_name.strip(), col_type.strip()))
            else:
                # 如果没有类型定义，默认为string
                columns.append((col_def.strip(), 'string'))
        
        return columns
    
    def get_timeseries_range(self, namespace: str, series_key: str, start_time: float = None, end_time: float = None, limit: int = None) -> List[Dict]:
        """
        获取时间序列数据范围
        
        Args:
            namespace: 命名空间
            series_key: 时间序列键名
            start_time: 开始时间戳（None表示从最早开始）
            end_time: 结束时间戳（None表示到最晚结束）
            limit: 限制返回数量
            
        Returns:
            时间序列数据列表，每个元素包含timestamp和value
        """
        redis_key = f"{namespace}::timeseries::{series_key}"
        
        # 设置查询范围
        min_score = start_time if start_time is not None else '-inf'
        max_score = end_time if end_time is not None else '+inf'
        
        # 获取数据
        if limit:
            results = self.redis_client.zrangebyscore(redis_key, min_score, max_score, withscores=True, start=0, num=limit)
        else:
            results = self.redis_client.zrangebyscore(redis_key, min_score, max_score, withscores=True)
        
        # 反序列化并格式化结果
        timeseries_data = []
        for value_str, timestamp in results:
            try:
                value_data = json.loads(value_str)
                timeseries_data.append({
                    "timestamp": timestamp,
                    "value": value_data
                })
            except json.JSONDecodeError:
                # 如果反序列化失败，使用原始字符串
                timeseries_data.append({
                    "timestamp": timestamp,
                    "value": value_str
                })
        
        return timeseries_data
    
    def get_densets_range(self, namespace: str, series_key: str, start_time: float = None, end_time: float = None, limit: int = None) -> List[Dict]:
        """
        获取densets数据范围
        
        Args:
            namespace: 命名空间
            series_key: densets键名
            start_time: 开始时间戳（None表示从最早开始）
            end_time: 结束时间戳（None表示到最晚结束）
            limit: 限制返回数量
            
        Returns:
            densets数据列表，每个元素包含timestamp和value
        """
        redis_key = f"{namespace}::densets::{series_key}"
        
        # 设置查询范围
        min_score = start_time if start_time is not None else '-inf'
        max_score = end_time if end_time is not None else '+inf'
        
        # 获取数据
        if limit:
            results = self.redis_client.zrangebyscore(redis_key, min_score, max_score, withscores=True, start=0, num=limit)
        else:
            results = self.redis_client.zrangebyscore(redis_key, min_score, max_score, withscores=True)
        
        # 格式化结果
        densets_data = []
        for value_str, timestamp in results:
            densets_data.append({
                "timestamp": timestamp,
                "value": value_str
            })
        
        return densets_data
    
    def get_timeseries_latest(self, namespace: str, series_key: str, count: int = 1) -> List[Dict]:
        """
        获取时间序列最新数据
        
        Args:
            namespace: 命名空间
            series_key: 时间序列键名
            count: 返回最新的数据点数量
            
        Returns:
            最新的时间序列数据列表
        """
        redis_key = f"{namespace}::timeseries::{series_key}"
        
        # 获取最新的数据（按时间戳降序）
        results = self.redis_client.zrevrange(redis_key, 0, count - 1, withscores=True)
        
        # 反序列化并格式化结果
        timeseries_data = []
        for value_str, timestamp in results:
            try:
                value_data = json.loads(value_str)
                timeseries_data.append({
                    "timestamp": timestamp,
                    "value": value_data
                })
            except json.JSONDecodeError:
                timeseries_data.append({
                    "timestamp": timestamp,
                    "value": value_str
                })
        
        return timeseries_data
    
    def get_densets_latest(self, namespace: str, series_key: str, count: int = 1) -> List[Dict]:
        """
        获取densets最新数据
        
        Args:
            namespace: 命名空间
            series_key: densets键名
            count: 返回最新的数据点数量
            
        Returns:
            最新的densets数据列表
        """
        redis_key = f"{namespace}::densets::{series_key}"
        
        # 获取最新的数据（按时间戳降序）
        results = self.redis_client.zrevrange(redis_key, 0, count - 1, withscores=True)
        
        # 格式化结果
        densets_data = []
        for value_str, timestamp in results:
            densets_data.append({
                "timestamp": timestamp,
                "value": value_str
            })
        
        return densets_data
    
    def get_timeseries_count(self, namespace: str, series_key: str, start_time: float = None, end_time: float = None) -> int:
        """
        获取时间序列数据点数量
        
        Args:
            namespace: 命名空间
            series_key: 时间序列键名
            start_time: 开始时间戳
            end_time: 结束时间戳
            
        Returns:
            数据点数量
        """
        redis_key = f"{namespace}::timeseries::{series_key}"
        
        if start_time is not None and end_time is not None:
            return self.redis_client.zcount(redis_key, start_time, end_time)
        else:
            return self.redis_client.zcard(redis_key)
    
    def get_densets_count(self, namespace: str, series_key: str, start_time: float = None, end_time: float = None) -> int:
        """
        获取densets数据点数量
        
        Args:
            namespace: 命名空间
            series_key: densets键名
            start_time: 开始时间戳
            end_time: 结束时间戳
            
        Returns:
            数据点数量
        """
        redis_key = f"{namespace}::densets::{series_key}"
        
        if start_time is not None and end_time is not None:
            return self.redis_client.zcount(redis_key, start_time, end_time)
        else:
            return self.redis_client.zcard(redis_key)
    
    def remove_timeseries_range(self, namespace: str, series_key: str, start_time: float, end_time: float) -> int:
        """
        删除时间序列指定时间范围的数据
        
        Args:
            namespace: 命名空间
            series_key: 时间序列键名
            start_time: 开始时间戳
            end_time: 结束时间戳
            
        Returns:
            删除的数据点数量
        """
        redis_key = f"{namespace}::timeseries::{series_key}"
        removed_count = self.redis_client.zremrangebyscore(redis_key, start_time, end_time)
        print(f"✅ 删除了 {removed_count} 个时间序列数据点")
        return removed_count
    
    def remove_densets_range(self, namespace: str, series_key: str, start_time: float, end_time: float) -> int:
        """
        删除densets指定时间范围的数据
        
        Args:
            namespace: 命名空间
            series_key: densets键名
            start_time: 开始时间戳
            end_time: 结束时间戳
            
        Returns:
            删除的数据点数量
        """
        redis_key = f"{namespace}::densets::{series_key}"
        removed_count = self.redis_client.zremrangebyscore(redis_key, start_time, end_time)
        print(f"✅ 删除了 {removed_count} 个densets数据点")
        return removed_count
    
    def cleanup_old_timeseries(self, namespace: str, series_key: str, keep_latest: int = 1000):
        """
        清理旧的时间序列数据，只保留最新的N个数据点
        
        Args:
            namespace: 命名空间
            series_key: 时间序列键名
            keep_latest: 保留最新的数据点数量
            
        Returns:
            删除的数据点数量
        """
        redis_key = f"{namespace}::timeseries::{series_key}"
        
        # 获取总数量
        total_count = self.redis_client.zcard(redis_key)
        
        if total_count <= keep_latest:
            return 0
        
        # 删除旧数据（保留最新的keep_latest个）
        removed_count = self.redis_client.zremrangebyrank(redis_key, 0, total_count - keep_latest - 1)
        print(f"✅ 清理了 {removed_count} 个旧的时间序列数据点")
        return removed_count
    
    def cleanup_old_densets(self, namespace: str, series_key: str, keep_latest: int = 1000):
        """
        清理旧的densets数据，只保留最新的N个数据点
        
        Args:
            namespace: 命名空间
            series_key: densets键名
            keep_latest: 保留最新的数据点数量
            
        Returns:
            删除的数据点数量
        """
        redis_key = f"{namespace}::densets::{series_key}"
        
        # 获取总数量
        total_count = self.redis_client.zcard(redis_key)
        
        if total_count <= keep_latest:
            return 0
        
        # 删除旧数据（保留最新的keep_latest个）
        removed_count = self.redis_client.zremrangebyrank(redis_key, 0, total_count - keep_latest - 1)
        print(f"✅ 清理了 {removed_count} 个旧的densets数据点")
        return removed_count
    

    # ========== 工具方法 ==========
    
    def get_ttl(self, key: str) -> int:
        """
        获取键的剩余生存时间
        
        Args:
            key: 键名
            
        Returns:
            剩余秒数，-1表示永不过期，-2表示键不存在
        """
        return self.redis_client.ttl(key)
    
    def list_keys(self, pattern: str = "*") -> List[str]:
        """列出所有匹配的键"""
        return self.redis_client.keys(pattern)
    
    def delete_key(self, key: str) -> bool:
        """删除指定键"""
        result = self.redis_client.delete(key)
        return result > 0
    
    def clear_all(self):
        """清空所有数据"""
        self.redis_client.flushdb()
        print("✅ 所有数据已清空")


if __name__ == "__main__":
    print("🚀 Redis连接器")
    print("💡 运行单元测试: python -m pytest test/ -v")
    print("💡 或者: cd test && python test_redis_connector.py")
    
    try:
        # 简单连接测试
        connector = RedisConnector()
        print("✅ Redis连接成功")
        
        # 测试JSON模块
        connector_json = RedisConnector(use_json_module=True)
        if connector_json.use_json_module:
            print("✅ RedisJSON模块可用")
        else:
            print("⚠️ RedisJSON模块不可用")
            
    except redis.ConnectionError:
        print("❌ 无法连接到Redis服务器")
        print("💡 请确保Redis服务器正在运行:")
        print("   - macOS: brew services start redis")
        print("   - Ubuntu: sudo systemctl start redis")
        print("   - 或直接运行: redis-server")
    except Exception as e:
        print(f"❌ 发生错误: {e}")
