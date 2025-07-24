import unittest
import sys
import os
import time
from unittest.mock import patch

# 添加父目录到路径以便导入RedisConnector
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.redis_connector import RedisConnector
import redis


class TestRedisTimeseriesOperations(unittest.TestCase):
    """Redis时间序列操作测试"""
    
    def setUp(self):
        """测试前准备"""
        try:
            self.connector = RedisConnector()
            self.connector.clear_all()
            self.base_time = time.time()
        except redis.ConnectionError:
            self.skipTest("Redis服务器未运行")
    
    def tearDown(self):
        """测试后清理"""
        if hasattr(self, 'connector'):
            self.connector.clear_all()
    
    def test_add_timeseries_point(self):
        """测试添加时间序列数据点"""
        # 测试添加简单数值
        timestamp = self.base_time
        value = 42
        
        self.connector.add_timeseries_point("test", "series1", timestamp, value)
        
        # 验证数据已添加
        data = self.connector.get_timeseries_range("test", "series1")
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["timestamp"], timestamp)
        self.assertEqual(data[0]["value"]["value"], value)
        
        # 测试添加复杂对象
        complex_value = {
            "temperature": 25.5,
            "humidity": 60.2,
            "location": "office"
        }
        
        timestamp2 = self.base_time + 60
        self.connector.add_timeseries_point("test", "series1", timestamp2, complex_value)
        
        data = self.connector.get_timeseries_range("test", "series1")
        self.assertEqual(len(data), 2)
        self.assertEqual(data[1]["value"], complex_value)
    
    def test_get_timeseries_range(self):
        """测试获取时间序列范围数据"""
        # 添加测试数据
        base_time = self.base_time
        for i in range(10):
            timestamp = base_time + i * 60  # 每分钟一个数据点
            value = {"counter": i, "value": i * 10}
            self.connector.add_timeseries_point("test", "range_test", timestamp, value)
        
        # 测试获取所有数据
        all_data = self.connector.get_timeseries_range("test", "range_test")
        self.assertEqual(len(all_data), 10)
        
        # 验证数据按时间戳排序
        for i, point in enumerate(all_data):
            self.assertEqual(point["timestamp"], base_time + i * 60)
            self.assertEqual(point["value"]["counter"], i)
        
        # 测试时间范围查询
        start_time = base_time + 120  # 从第3个数据点开始
        end_time = base_time + 480    # 到第9个数据点结束
        
        range_data = self.connector.get_timeseries_range("test", "range_test", start_time, end_time)
        self.assertEqual(len(range_data), 7)  # 应该有7个数据点
        self.assertEqual(range_data[0]["value"]["counter"], 2)
        self.assertEqual(range_data[-1]["value"]["counter"], 8)
        
        # 测试限制数量
        limited_data = self.connector.get_timeseries_range("test", "range_test", limit=5)
        self.assertEqual(len(limited_data), 5)
    
    def test_get_timeseries_latest(self):
        """测试获取最新时间序列数据"""
        # 添加测试数据
        base_time = self.base_time
        for i in range(5):
            timestamp = base_time + i * 60
            value = {"index": i, "data": f"item_{i}"}
            self.connector.add_timeseries_point("test", "latest_test", timestamp, value)
        
        # 测试获取最新1个数据点
        latest_one = self.connector.get_timeseries_latest("test", "latest_test", count=1)
        self.assertEqual(len(latest_one), 1)
        self.assertEqual(latest_one[0]["value"]["index"], 4)  # 最新的是index=4
        
        # 测试获取最新3个数据点
        latest_three = self.connector.get_timeseries_latest("test", "latest_test", count=3)
        self.assertEqual(len(latest_three), 3)
        
        # 验证按时间戳降序排列
        self.assertEqual(latest_three[0]["value"]["index"], 4)
        self.assertEqual(latest_three[1]["value"]["index"], 3)
        self.assertEqual(latest_three[2]["value"]["index"], 2)
    
    def test_get_timeseries_count(self):
        """测试获取时间序列数据点数量"""
        # 添加测试数据
        base_time = self.base_time
        for i in range(8):
            timestamp = base_time + i * 60
            self.connector.add_timeseries_point("test", "count_test", timestamp, i)
        
        # 测试获取总数量
        total_count = self.connector.get_timeseries_count("test", "count_test")
        self.assertEqual(total_count, 8)
        
        # 测试获取特定时间范围的数量
        start_time = base_time + 120  # 从第3个数据点开始
        end_time = base_time + 360    # 到第7个数据点结束
        
        range_count = self.connector.get_timeseries_count("test", "count_test", start_time, end_time)
        self.assertEqual(range_count, 5)  # 应该有5个数据点
    
    def test_remove_timeseries_range(self):
        """测试删除时间序列范围数据"""
        # 添加测试数据
        base_time = self.base_time
        for i in range(10):
            timestamp = base_time + i * 60
            self.connector.add_timeseries_point("test", "remove_test", timestamp, i)
        
        # 验证初始数据
        initial_count = self.connector.get_timeseries_count("test", "remove_test")
        self.assertEqual(initial_count, 10)
        
        # 删除中间的数据点
        start_time = base_time + 180  # 第4个数据点
        end_time = base_time + 420    # 第8个数据点
        
        removed_count = self.connector.remove_timeseries_range("test", "remove_test", start_time, end_time)
        self.assertEqual(removed_count, 5)  # 应该删除5个数据点
        
        # 验证剩余数据
        remaining_count = self.connector.get_timeseries_count("test", "remove_test")
        self.assertEqual(remaining_count, 5)
        
        # 验证剩余的数据是正确的
        remaining_data = self.connector.get_timeseries_range("test", "remove_test")
        self.assertEqual(len(remaining_data), 5)
        
        # 前3个和后2个数据点应该还在
        self.assertEqual(remaining_data[0]["value"]["value"], 0)
        self.assertEqual(remaining_data[1]["value"]["value"], 1)
        self.assertEqual(remaining_data[2]["value"]["value"], 2)
        self.assertEqual(remaining_data[3]["value"]["value"], 8)
        self.assertEqual(remaining_data[4]["value"]["value"], 9)
    
    def test_cleanup_old_timeseries(self):
        """测试清理旧的时间序列数据"""
        # 添加测试数据
        base_time = self.base_time
        for i in range(20):
            timestamp = base_time + i * 60
            self.connector.add_timeseries_point("test", "cleanup_test", timestamp, i)
        
        # 验证初始数据
        initial_count = self.connector.get_timeseries_count("test", "cleanup_test")
        self.assertEqual(initial_count, 20)
        
        # 清理数据，只保留最新的5个
        removed_count = self.connector.cleanup_old_timeseries("test", "cleanup_test", keep_latest=5)
        self.assertEqual(removed_count, 15)  # 应该删除15个数据点
        
        # 验证剩余数据
        remaining_count = self.connector.get_timeseries_count("test", "cleanup_test")
        self.assertEqual(remaining_count, 5)
        
        # 验证保留的是最新的5个数据点
        remaining_data = self.connector.get_timeseries_range("test", "cleanup_test")
        self.assertEqual(len(remaining_data), 5)
        
        for i, point in enumerate(remaining_data):
            expected_value = 15 + i  # 应该是15, 16, 17, 18, 19
            self.assertEqual(point["value"]["value"], expected_value)
        
        # 测试当数据量少于保留数量时
        removed_count_2 = self.connector.cleanup_old_timeseries("test", "cleanup_test", keep_latest=10)
        self.assertEqual(removed_count_2, 0)  # 不应该删除任何数据
        
        final_count = self.connector.get_timeseries_count("test", "cleanup_test")
        self.assertEqual(final_count, 5)  # 数量应该保持不变
    
    def test_timeseries_ttl(self):
        """测试时间序列TTL功能"""
        # 添加带自定义TTL的数据
        timestamp = self.base_time
        value = {"test": "ttl_data"}
        custom_ttl = 3600  # 1小时
        
        self.connector.add_timeseries_point("test", "ttl_test", timestamp, value, ttl=custom_ttl)
        
        # 检查TTL是否设置正确
        ttl = self.connector.get_ttl("test::timeseries::ttl_test")
        self.assertTrue(ttl > 0)
        self.assertTrue(ttl <= custom_ttl)
    
    def test_timeseries_error_handling(self):
        """测试时间序列错误处理"""
        # 测试获取不存在的时间序列
        empty_data = self.connector.get_timeseries_range("nonexistent", "series")
        self.assertEqual(empty_data, [])
        
        # 测试获取不存在序列的最新数据
        empty_latest = self.connector.get_timeseries_latest("nonexistent", "series")
        self.assertEqual(empty_latest, [])
        
        # 测试获取不存在序列的数量
        zero_count = self.connector.get_timeseries_count("nonexistent", "series")
        self.assertEqual(zero_count, 0)
        
        # 测试删除不存在的时间序列范围
        removed = self.connector.remove_timeseries_range("nonexistent", "series", self.base_time, self.base_time + 3600)
        self.assertEqual(removed, 0)
    
    def test_timeseries_data_types(self):
        """测试时间序列数据类型处理"""
        base_time = self.base_time
        
        # 测试不同类型的数据
        test_cases = [
            {"timestamp": base_time, "value": 42},
            {"timestamp": base_time + 60, "value": 3.14},
            {"timestamp": base_time + 120, "value": "string_value"},
            {"timestamp": base_time + 180, "value": True},
            {"timestamp": base_time + 240, "value": [1, 2, 3]},
            {"timestamp": base_time + 300, "value": {"nested": {"data": "complex"}}},
        ]
        
        # 添加所有测试数据
        for case in test_cases:
            self.connector.add_timeseries_point("test", "types_test", case["timestamp"], case["value"])
        
        # 获取所有数据并验证
        all_data = self.connector.get_timeseries_range("test", "types_test")
        self.assertEqual(len(all_data), len(test_cases))
        
        for i, point in enumerate(all_data):
            expected_case = test_cases[i]
            self.assertEqual(point["timestamp"], expected_case["timestamp"])
            
            # 对于简单类型，它们会被包装在{"value": ...}中
            if isinstance(expected_case["value"], (int, float, str, bool)):
                self.assertEqual(point["value"]["value"], expected_case["value"])
            else:
                # 对于复杂类型，直接比较
                self.assertEqual(point["value"], expected_case["value"])
    
    def test_multiple_timeseries(self):
        """测试多个时间序列"""
        base_time = self.base_time
        
        # 创建多个时间序列
        series_names = ["sensor_1", "sensor_2", "sensor_3"]
        
        for series_name in series_names:
            for i in range(5):
                timestamp = base_time + i * 60
                value = {"sensor": series_name, "reading": i * 10}
                self.connector.add_timeseries_point("sensors", series_name, timestamp, value)
        
        # 验证每个时间序列的数据
        for series_name in series_names:
            data = self.connector.get_timeseries_range("sensors", series_name)
            self.assertEqual(len(data), 5)
            
            for i, point in enumerate(data):
                self.assertEqual(point["value"]["sensor"], series_name)
                self.assertEqual(point["value"]["reading"], i * 10)
        
        # 验证时间序列之间的独立性
        self.connector.remove_timeseries_range("sensors", "sensor_1", base_time, base_time + 120)
        
        # sensor_1应该只剩2个数据点 (删除了前3个: 0, 60, 120秒的数据点)
        sensor_1_data = self.connector.get_timeseries_range("sensors", "sensor_1")
        self.assertEqual(len(sensor_1_data), 2)
        
        # 其他传感器应该不受影响
        for series_name in ["sensor_2", "sensor_3"]:
            data = self.connector.get_timeseries_range("sensors", series_name)
            self.assertEqual(len(data), 5)


if __name__ == '__main__':
    unittest.main(verbosity=2) 