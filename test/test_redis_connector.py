import unittest
import sys
import os
import time
from unittest.mock import patch

# 添加父目录到路径以便导入RedisConnector
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.redis_connector import RedisConnector
import redis


class TestRedisConnector(unittest.TestCase):
    """Redis连接器单元测试"""
    
    def setUp(self):
        """测试前准备"""
        try:
            self.connector = RedisConnector()
            self.connector.clear_all()
        except redis.ConnectionError:
            self.skipTest("Redis服务器未运行")
    
    def tearDown(self):
        """测试后清理"""
        if hasattr(self, 'connector'):
            self.connector.clear_all()
    
    def test_json_variable_operations(self):
        """测试JSON变量的存储和读取"""
        # 测试用户数据存储
        user_data = {
            "name": "张三",
            "age": 25,
            "email": "zhangsan@example.com",
            "preferences": {
                "theme": "dark",
                "language": "zh-CN"
            }
        }
        
        # 存储JSON变量
        self.connector.store_json_variable("user", "user_123_club_456", user_data)
        
        # 读取JSON变量
        retrieved_data = self.connector.get_json_variable("user", "user_123_club_456")
        
        # 验证数据完整性
        self.assertEqual(retrieved_data["name"], "张三")
        self.assertEqual(retrieved_data["age"], 25)
        self.assertEqual(retrieved_data["email"], "zhangsan@example.com")
        self.assertEqual(retrieved_data["preferences"]["theme"], "dark")
        self.assertEqual(retrieved_data["preferences"]["language"], "zh-CN")
        
        # 测试产品数据存储
        product_data = {
            "name": "智能手机",
            "price": 2999.99,
            "stock": 100,
            "features": ["5G", "双摄", "快充"]
        }
        
        self.connector.store_json_variable("product", "P001_electronics", product_data)
        retrieved_product = self.connector.get_json_variable("product", "P001_electronics")
        
        self.assertEqual(retrieved_product["name"], "智能手机")
        self.assertEqual(retrieved_product["price"], 2999.99)
        self.assertEqual(retrieved_product["stock"], 100)
        self.assertEqual(len(retrieved_product["features"]), 3)
        
        # 测试TTL
        user_ttl = self.connector.get_ttl("user::json::user_123_club_456")
        self.assertTrue(user_ttl > 0)  # 应该有TTL
        self.assertTrue(user_ttl <= 432000)  # 不应该超过默认TTL
    
    def test_direct_variable_operations(self):
        """测试直接变量的存储和读取"""
        # 测试字符串变量
        self.connector.store_direct_variable("config", "myapp_production_database_url", "mysql://localhost:3306/mydb")
        database_url = self.connector.get_direct_variable("config", "myapp_production_database_url")
        self.assertEqual(database_url, "mysql://localhost:3306/mydb")
        
        # 测试整数变量
        self.connector.store_direct_variable("config", "myapp_production_max_connections", 100)
        max_connections = self.connector.get_direct_variable("config", "myapp_production_max_connections")
        self.assertEqual(max_connections, "100")  # Redis存储为字符串
        
        # 测试浮点数变量
        self.connector.store_direct_variable("config", "myapp_production_timeout", 30.5)
        timeout = self.connector.get_direct_variable("config", "myapp_production_timeout")
        self.assertEqual(timeout, "30.5")
        
        # 测试布尔变量
        self.connector.store_direct_variable("config", "myapp_production_debug_mode", True)
        debug_mode = self.connector.get_direct_variable("config", "myapp_production_debug_mode")
        self.assertEqual(debug_mode, "True")
        
        # 测试自定义TTL
        self.connector.store_direct_variable("config", "temp_setting", "temporary", ttl=600)
        temp_ttl = self.connector.get_ttl("config::value::temp_setting")
        self.assertTrue(temp_ttl > 0)
        self.assertTrue(temp_ttl <= 600)
    
    def test_custom_ttl_operations(self):
        """测试自定义TTL功能"""
        # 测试自定义TTL的JSON变量
        test_data = {"test": "data"}
        self.connector.store_json_variable("test", "custom_ttl_test", test_data, ttl=3600)
        
        # 检查TTL
        ttl = self.connector.get_ttl("test::json::custom_ttl_test")
        self.assertTrue(ttl > 0)
        self.assertTrue(ttl <= 3600)
        
        # 测试自定义TTL的直接变量
        self.connector.store_direct_variable("test", "custom_ttl_direct", "test_value", ttl=1800)
        ttl_direct = self.connector.get_ttl("test::value::custom_ttl_direct")
        self.assertTrue(ttl_direct > 0)
        self.assertTrue(ttl_direct <= 1800)
    
    def test_utility_functions(self):
        """测试工具函数"""
        # 存储一些测试数据
        self.connector.store_json_variable("user", "test_user", {"name": "test"})
        self.connector.store_direct_variable("config", "test_config", "test_value")
        
        # 测试列出所有键
        all_keys = self.connector.list_keys()
        self.assertIsInstance(all_keys, list)
        self.assertTrue(len(all_keys) >= 2)
        
        # 测试模式匹配
        user_keys = self.connector.list_keys("user::*")
        self.assertTrue(len(user_keys) >= 1)
        self.assertTrue(any("user::json::test_user" in key for key in user_keys))
        
        config_keys = self.connector.list_keys("config::*")
        self.assertTrue(len(config_keys) >= 1)
        self.assertTrue(any("config::value::test_config" in key for key in config_keys))
        
        # 测试删除键
        if user_keys:
            deleted = self.connector.delete_key(user_keys[0])
            self.assertTrue(deleted)
            
            # 验证键已被删除
            updated_keys = self.connector.list_keys("user::*")
            self.assertEqual(len(updated_keys), len(user_keys) - 1)
    
    def test_error_handling(self):
        """测试错误处理"""
        # 测试获取不存在的JSON变量
        with self.assertRaises(KeyError):
            self.connector.get_json_variable("nonexistent", "key")
        
        # 测试获取不存在的直接变量
        with self.assertRaises(KeyError):
            self.connector.get_direct_variable("nonexistent", "key")
        
        # 测试删除不存在的键
        deleted = self.connector.delete_key("nonexistent_key")
        self.assertFalse(deleted)
    
    def test_data_types_and_serialization(self):
        """测试数据类型和序列化"""
        # 测试复杂JSON结构
        complex_data = {
            "string": "测试字符串",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "null": None,
            "list": [1, 2, 3, "test"],
            "nested": {
                "deep": {
                    "value": "深层嵌套"
                }
            }
        }
        
        self.connector.store_json_variable("test", "complex_data", complex_data)
        retrieved = self.connector.get_json_variable("test", "complex_data")
        
        # 验证所有数据类型
        self.assertEqual(retrieved["string"], "测试字符串")
        self.assertEqual(retrieved["number"], 42)
        self.assertEqual(retrieved["float"], 3.14)
        self.assertEqual(retrieved["boolean"], True)
        self.assertIsNone(retrieved["null"])
        self.assertEqual(retrieved["list"], [1, 2, 3, "test"])
        self.assertEqual(retrieved["nested"]["deep"]["value"], "深层嵌套")


class TestRedisConnectorWithJSONModule(unittest.TestCase):
    """测试RedisJSON模块功能"""
    
    def setUp(self):
        """测试前准备"""
        try:
            self.connector = RedisConnector(use_json_module=True)
            if not self.connector.use_json_module:
                self.skipTest("RedisJSON模块不可用")
            self.connector.clear_all()
        except redis.ConnectionError:
            self.skipTest("Redis服务器未运行")
    
    def tearDown(self):
        """测试后清理"""
        if hasattr(self, 'connector'):
            self.connector.clear_all()
    
    def test_json_module_basic_operations(self):
        """测试JSON模块基本操作"""
        test_data = {
            "user_id": "12345",
            "profile": {
                "name": "测试用户",
                "age": 25,
                "preferences": {
                    "theme": "dark",
                    "language": "zh-CN"
                }
            }
        }
        
        # 存储和读取
        self.connector.store_json_variable("test", "json_module_test", test_data)
        retrieved = self.connector.get_json_variable("test", "json_module_test")
        
        self.assertEqual(retrieved["user_id"], "12345")
        self.assertEqual(retrieved["profile"]["name"], "测试用户")
        self.assertEqual(retrieved["profile"]["preferences"]["theme"], "dark")
    
    def test_json_field_operations(self):
        """测试JSON字段级操作"""
        test_data = {
            "profile": {
                "name": "测试用户",
                "preferences": {
                    "theme": "dark",
                    "language": "zh-CN"
                }
            }
        }
        
        self.connector.store_json_variable("test", "field_test", test_data)
        
        # 测试获取特定字段
        name = self.connector.get_json_field("test", "field_test", ".profile.name")
        self.assertEqual(name, "测试用户")
        
        theme = self.connector.get_json_field("test", "field_test", ".profile.preferences.theme")
        self.assertEqual(theme, "dark")
        
        # 测试更新特定字段
        self.connector.update_json_field("test", "field_test", ".profile.preferences.theme", "light")
        updated_theme = self.connector.get_json_field("test", "field_test", ".profile.preferences.theme")
        self.assertEqual(updated_theme, "light")
        
        # 验证其他字段未受影响
        name_after_update = self.connector.get_json_field("test", "field_test", ".profile.name")
        self.assertEqual(name_after_update, "测试用户")
    
    def test_json_field_error_handling(self):
        """测试JSON字段操作错误处理"""
        test_data = {"field": "value"}
        self.connector.store_json_variable("test", "error_test", test_data)
        
        # 测试获取不存在的字段
        with self.assertRaises(KeyError):
            self.connector.get_json_field("test", "error_test", ".nonexistent")
        
        # 测试获取不存在的键
        with self.assertRaises(KeyError):
            self.connector.get_json_field("test", "nonexistent_key", ".field")


class TestRedisConnectorPerformance(unittest.TestCase):
    """性能测试"""
    
    def setUp(self):
        """测试前准备"""
        try:
            self.connector_string = RedisConnector(use_json_module=False)
            self.connector_json = RedisConnector(use_json_module=True)
            self.connector_string.clear_all()
            if self.connector_json.use_json_module:
                self.connector_json.clear_all()
        except redis.ConnectionError:
            self.skipTest("Redis服务器未运行")
    
    def tearDown(self):
        """测试后清理"""
        if hasattr(self, 'connector_string'):
            self.connector_string.clear_all()
        if hasattr(self, 'connector_json') and self.connector_json.use_json_module:
            self.connector_json.clear_all()
    
    def test_performance_comparison(self):
        """测试性能对比"""
        # 创建测试数据
        test_data = {
            "user_id": "12345",
            "profile": {
                "name": "测试用户",
                "age": 25,
                "preferences": {
                    "theme": "dark",
                    "language": "zh-CN"
                }
            },
            "items": [f"item_{i}" for i in range(50)]  # 减少数据量以加快测试
        }
        
        # 字符串模式性能测试
        start_time = time.time()
        for i in range(50):  # 减少测试次数
            self.connector_string.store_json_variable("perf_test", f"user_{i}", test_data)
        string_store_time = time.time() - start_time
        
        start_time = time.time()
        for i in range(50):
            data = self.connector_string.get_json_variable("perf_test", f"user_{i}")
        string_read_time = time.time() - start_time
        
        # 基本断言
        self.assertGreater(string_store_time, 0)
        self.assertGreater(string_read_time, 0)
        
        # 如果JSON模块可用，进行对比测试
        if self.connector_json.use_json_module:
            start_time = time.time()
            for i in range(50):
                self.connector_json.store_json_variable("perf_test", f"user_{i}", test_data)
            json_store_time = time.time() - start_time
            
            start_time = time.time()
            for i in range(50):
                data = self.connector_json.get_json_variable("perf_test", f"user_{i}")
            json_read_time = time.time() - start_time
            
            # 性能断言（JSON模块通常更快，但不是绝对的）
            self.assertGreater(json_store_time, 0)
            self.assertGreater(json_read_time, 0)
            
            # 打印性能对比结果
            print(f"\n性能对比结果:")
            print(f"字符串模式 - 存储: {string_store_time:.3f}s, 读取: {string_read_time:.3f}s")
            print(f"JSON模块 - 存储: {json_store_time:.3f}s, 读取: {json_read_time:.3f}s")


class TestRedisConnectorConnection(unittest.TestCase):
    """连接测试"""
    
    def test_connection_success(self):
        """测试成功连接"""
        try:
            connector = RedisConnector()
            self.assertIsNotNone(connector.redis_client)
        except redis.ConnectionError:
            self.skipTest("Redis服务器未运行")
    
    def test_connection_with_json_module(self):
        """测试JSON模块连接"""
        try:
            connector = RedisConnector(use_json_module=True)
            self.assertIsNotNone(connector.redis_client)
            # use_json_module可能为True或False，取决于模块是否可用
            self.assertIsInstance(connector.use_json_module, bool)
        except redis.ConnectionError:
            self.skipTest("Redis服务器未运行")
    
    @patch('redis.Redis')
    def test_connection_failure(self, mock_redis):
        """测试连接失败"""
        mock_redis.return_value.ping.side_effect = redis.ConnectionError("Connection failed")
        
        with self.assertRaises(redis.ConnectionError):
            RedisConnector()


if __name__ == '__main__':
    # 运行所有测试
    unittest.main(verbosity=2) 