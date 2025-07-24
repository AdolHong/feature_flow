#!/usr/bin/env python3
"""
DataLoader 单元测试
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.redis_connector import RedisConnector
from data.dataloader import DataLoader

JOB_DATETIME = "2025-07-01 10:00:00"


class TestDataLoader:
    """DataLoader测试类"""
    
    @pytest.fixture
    def connector(self):
        """创建Redis连接器"""
        connector = RedisConnector()
        connector.clear_all()  # 清空测试数据
        return connector
    
    @pytest.fixture
    def loader(self, connector):
        """创建DataLoader实例"""
        return DataLoader(connector)
    
    def setup_demo_data(self, connector: RedisConnector):
        """设置演示数据"""
        print("🔧 设置演示数据...")
        
        # 清空现有数据
        connector.clear_all()
        
        # 1. 设置简单变量数据
        connector.store_direct_variable("sams_demand_forecast", "cloud_nbr=1001::city_cn", "深圳")
        
        # 2. 设置JSON变量数据
        store_info = {
            "mother_store_nbr": "1001",
            "city_en": "Shenzhen",
            "city_cn": "深圳"
        }
        connector.store_json_variable("sams_demand_forecast", "cloud_nbr=1001::dim_store", store_info)
        
        # 3. 设置时间序列数据
        base_time = datetime(2024, 1, 1)
        for i in range(10):
            current_date = base_time + timedelta(days=i)
            ds = current_date.strftime("%Y-%m-%d")
            etl_time = (current_date + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            
            data = {
                "ds": ds,
                "etl_time": etl_time,
                "order_quantity": 100 + i * 10,
                "is_activate": 1 if i % 2 == 0 else 0
            }
            
            connector.add_timeseries_point(
                "sams_demand_forecast",
                "cloud_nbr=1001::item_nbr=2001::order_quantity",
                current_date.timestamp(),
                data
            )
        
        # 4. 设置另一个时间序列数据
        for i in range(10):
            current_date = base_time + timedelta(days=i)
            ds = current_date.strftime("%Y-%m-%d")
            etl_time = (current_date + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            
            data = {
                "ds": ds,
                "etl_time": etl_time,
                "id1": 50.5 + i * 5.2,
                "id2": 100.5 + i * 5.2,
                "id3": 150.5 + i * 5.2,
            }
            
            connector.add_timeseries_point(
                "sams_demand_forecast",
                "cloud_nbr=1001::job_date=2024-01-01::same_city_forecast",
                current_date.timestamp(),
                data
            )
        
        # 5. 添加一些重复数据来测试去重功能
        duplicate_date = base_time + timedelta(days=2)
        duplicate_ds = duplicate_date.strftime("%Y-%m-%d")
        old_etl_time = (duplicate_date + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        new_etl_time = (duplicate_date + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
        
        # 添加较早的数据
        old_data = {
            "ds": duplicate_ds,
            "etl_time": old_etl_time,
            "order_quantity": 999,
            "is_activate": 0
        }
        connector.add_timeseries_point(
            "sams_demand_forecast",
            "cloud_nbr=1001::item_nbr=2001::order_quantity",
            duplicate_date.timestamp(),
            old_data
        )
        
        # 添加较新的数据
        new_data = {
            "ds": duplicate_ds,
            "etl_time": new_etl_time,
            "order_quantity": 120,
            "is_activate": 1
        }
        connector.add_timeseries_point(
            "sams_demand_forecast",
            "cloud_nbr=1001::item_nbr=2001::order_quantity",
            duplicate_date.timestamp(),
            new_data
        )
        
        print("✅ 演示数据设置完成")
    
    def get_demo_config(self):
        """获取演示配置"""
        return {
            "namespace": "sams_demand_forecast",
            "placeholder": [
                {"name": "cloud_nbr", "alias": None, "description": "山姆极速达云仓的仓号"},
                {"name": "item_nbr", "alias": None, "description": "商品号"},
                {"name": "biz_date", "alias": None, "description": "预测日期"},
            ],
            "variable": [
                {
                    "name": "df_quantity",
                    "alias": None,
                    "description": None,
                    "namespace": None,
                    "redis_config": {
                        "prefix": [
                            {"key": "cloud_nbr", "value": "${cloud_nbr}"},
                            {"key": "item_nbr", "value": "${item_nbr}"}
                        ],
                        "field": "order_quantity",
                        "type": "timeseries",
                        "from_datetime": "${yyyy-MM-dd-60d}",
                        "to_datetime": "${yyyy-MM-dd-1d}",
                        "drop_duplicate": "keep_latest"
                    }
                },
                {
                    "name": "city_cn",
                    "alias": None,
                    "description": None,
                    "namespace": None,
                    "redis_config": {
                        "prefix": [
                            {"key": "cloud_nbr", "value": "${cloud_nbr}"}
                        ],
                        "field": "city_cn",
                        "type": "value"
                    }
                },
                {
                    "name": "same_city_forecast",
                    "alias": None,
                    "description": None,
                    "namespace": None,
                    "redis_config": {
                        "prefix": [
                            {"key": "cloud_nbr", "value": "${cloud_nbr}"},
                            {"key": "job_date", "value": "${yyyy-MM-dd}"}
                        ],
                        "field": "same_city_forecast",
                        "type": "timeseries",
                        "from_datetime": "${yyyy-MM-dd}",
                        "to_datetime": "${yyyy-MM-dd+13d}",
                        "drop_duplicate": "none"
                    }
                }
            ]
        }
    
    def test_placeholder_validation(self, loader):
        """测试占位符验证"""
        data_config = {
            "namespace": "test",
            "placeholder": [
                {"name": "param1", "alias": None, "description": "参数1"},
                {"name": "param2", "alias": None, "description": "参数2"}
            ],
            "variable": []
        }
        
        # 测试缺少占位符
        with pytest.raises(ValueError, match="缺少必需的占位符"):
            loader.load_data(data_config, {"param1": "value1"}, job_datetime=JOB_DATETIME)
        
        # 测试完整占位符
        result = loader.load_data(data_config, {"param1": "value1", "param2": "value2"}, job_datetime=JOB_DATETIME)
        assert result == {}
    
    def test_redis_key_building(self, loader):
        """测试Redis键构建"""
        prefixes = [
            {"key": "cloud_nbr", "value": "${cloud_nbr}"},
            {"key": "item_nbr", "value": "${item_nbr}"}
        ]
        placeholders = {"cloud_nbr": "1001", "item_nbr": "2001"}
        
        redis_key = loader._build_redis_key("test_ns", "timeseries", prefixes, "test_field", placeholders, JOB_DATETIME)
        expected_key = "test_ns::timeseries::cloud_nbr=1001::item_nbr=2001::test_field"
        assert redis_key == expected_key
    
    def test_placeholder_replacement(self, loader):
        """测试占位符替换"""
        placeholders = {"name": "test", "value": "123"}
        
        # 测试正常替换
        result = loader._replace_placeholders("${name}_${value}", placeholders)
        assert result == "test_123"
        
        # 测试缺少占位符
        with pytest.raises(ValueError, match="占位符 'missing' 未找到"):
            loader._replace_placeholders("${name}_${missing}", placeholders)
    
    def test_load_simple_variable_value(self, connector, loader):
        """测试加载简单变量（value类型）"""
        # 设置测试数据
        connector.store_direct_variable("test_ns", "cloud_nbr=1001::test_field", "test_value")
        
        # 配置
        redis_config = {
            "prefix": [{"key": "cloud_nbr", "value": "${cloud_nbr}"}],
            "field": "test_field",
            "type": "value"
        }
        
        # 加载数据
        result = loader._load_simple_variable(redis_config, "test_ns", {"cloud_nbr": "1001"}, job_datetime=JOB_DATETIME)
        assert result == "test_value"
    
    def test_load_simple_variable_json(self, connector, loader):
        """测试加载简单变量（json类型）"""
        # 设置测试数据
        test_data = {"name": "test", "value": 123}
        connector.store_json_variable("test_ns", "cloud_nbr=1001::test_field", test_data)
        
        # 配置
        redis_config = {
            "prefix": [{"key": "cloud_nbr", "value": "${cloud_nbr}"}],
            "field": "test_field",
            "type": "json"
        }
        
        # 加载数据
        result = loader._load_simple_variable(redis_config, "test_ns", {"cloud_nbr": "1001"}, job_datetime=JOB_DATETIME)
        assert result == test_data
    
    def test_load_timeseries(self, connector, loader):
        """测试加载时间序列"""
        # 设置测试数据
        base_time = datetime(2024, 1, 1)
        for i in range(3):
            current_date = base_time + timedelta(days=i)
            ds = current_date.strftime("%Y-%m-%d")
            etl_time = (current_date + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            data = {
                "ds": ds,
                "etl_time": etl_time,
                "quantity": 100 + i * 10,
                "status": i % 2
            }
            connector.add_timeseries_point("test_ns", "cloud_nbr=1001::test_series", current_date.timestamp(), data)
        
        # 配置
        variable_config = {
            "redis_config": {
                "prefix": [{"key": "cloud_nbr", "value": "${cloud_nbr}"}],
                "field": "test_series",
                "type": "timeseries"
            }
        }
        
        # 加载数据
        result = loader._load_timeseries(variable_config, "test_ns", {"cloud_nbr": "1001"}, job_datetime=JOB_DATETIME)
        
        # 验证结果
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert "ds" in result.columns
        assert "etl_time" in result.columns
        assert "quantity" in result.columns
        assert "status" in result.columns
        
        # 验证数据值
        assert result.iloc[0]["quantity"] == 100
        assert result.iloc[1]["quantity"] == 110
        assert result.iloc[2]["quantity"] == 120
        
        # 验证时间字段类型
        assert isinstance(result.iloc[0]["ds"], str)
        assert isinstance(result.iloc[0]["etl_time"], str)
    
    def test_timeseries_deduplication(self, connector, loader):
        """测试时间序列去重功能"""
        # 设置重复数据
        base_date = datetime(2024, 1, 1)
        ds = base_date.strftime("%Y-%m-%d")
        
        # 添加较早的数据
        old_etl_time = (base_date + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        old_data = {
            "ds": ds,
            "etl_time": old_etl_time,
            "id1": 100
        }
        connector.add_timeseries_point("test_ns", "test_series", base_date.timestamp(), old_data)
        
        # 添加较新的数据
        new_etl_time = (base_date + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        new_data = {
            "ds": ds,
            "etl_time": new_etl_time,
            "id1": 200
        }
        connector.add_timeseries_point("test_ns", "test_series", base_date.timestamp(), new_data)
        
        # 配置
        variable_config = {
            "redis_config": {
                "prefix": [],
                "field": "test_series",
                "type": "timeseries",
                "drop_duplicate": "keep_latest"
            }
        }
        
        # 加载数据
        result = loader._load_timeseries(variable_config, "test_ns", {}, job_datetime=JOB_DATETIME)
        
        # 验证去重效果：应该只有一条记录，且是较新的数据
        assert len(result) == 1
        assert result.iloc[0]["id1"] == 200
        assert isinstance(result.iloc[0]["etl_time"], str)
        assert "ds" in result.columns
    
    def test_load_data_integration(self, connector, loader):
        """测试完整的数据加载流程"""
        # 设置各种类型的测试数据
        connector.store_direct_variable("test_ns", "cloud_nbr=1001::city", "深圳")
        connector.store_json_variable("test_ns", "cloud_nbr=1001::info", {"name": "test", "id": 123})
        
        base_date = datetime(2024, 1, 1)
        ds = base_date.strftime("%Y-%m-%d")
        etl_time = (base_date + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        connector.add_timeseries_point("test_ns", "cloud_nbr=1001::series", base_date.timestamp(), {
            "ds": ds,
            "etl_time": etl_time,
            "id1": 100.5
        })
        
        # 配置
        data_config = {
            "namespace": "test_ns",
            "placeholder": [
                {"name": "cloud_nbr", "alias": None, "description": "云仓号"}
            ],
            "variable": [
                {
                    "name": "city",
                    "redis_config": {
                        "prefix": [{"key": "cloud_nbr", "value": "${cloud_nbr}"}],
                        "field": "city",
                        "type": "value"
                    }
                },
                {
                    "name": "info",
                    "redis_config": {
                        "prefix": [{"key": "cloud_nbr", "value": "${cloud_nbr}"}],
                        "field": "info",
                        "type": "json"
                    }
                },
                {
                    "name": "series",
                    "redis_config": {
                        "prefix": [{"key": "cloud_nbr", "value": "${cloud_nbr}"}],
                        "field": "series",
                        "type": "timeseries"
                    }
                }
            ]
        }
        
        # 加载数据
        result = loader.load_data(data_config, {"cloud_nbr": "1001"}, job_datetime=JOB_DATETIME)
        
        # 验证结果
        assert "city" in result
        assert "info" in result
        assert "series" in result
        
        assert result["city"] == "深圳"
        assert result["info"] == {"name": "test", "id": 123}
        assert isinstance(result["series"], pd.DataFrame)
        assert len(result["series"]) == 1
        assert result["series"].iloc[0]["id1"] == 100.5
        
        # 验证时间序列字段
        assert "ds" in result["series"].columns
        assert "etl_time" in result["series"].columns
    
    def test_timeseries_ds_etl_time_fields(self, connector, loader):
        """测试时间序列ds和etl_time字段"""
        # 设置测试数据
        base_date = datetime(2024, 1, 1, 12, 30, 45)
        ds = base_date.strftime("%Y-%m-%d")
        etl_time = (base_date + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        connector.add_timeseries_point("test_ns", "test_series", base_date.timestamp(), {
            "ds": ds,
            "etl_time": etl_time,
            "id1": 100.5
        })
        
        # 测试配置
        variable_config = {
            "redis_config": {
                "prefix": [],
                "field": "test_series",
                "type": "timeseries"
            }
        }
        
        result = loader._load_timeseries(variable_config, "test_ns", {}, job_datetime=JOB_DATETIME)
        
        # 验证ds和etl_time字段
        assert len(result) == 1
        assert result.iloc[0]["ds"] == ds
        assert result.iloc[0]["etl_time"] == etl_time
        assert result.iloc[0]["id1"] == 100.5
    
    def test_drop_duplicate_strategies(self, connector, loader):
        """测试不同的去重策略"""
        # 设置重复数据
        base_date = datetime(2024, 1, 1)
        timestamp = base_date.timestamp()
        
        # 添加较早的数据
        old_etl_time = (base_date + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        old_data = {
            "ds": "2024-01-01",
            "etl_time": old_etl_time,
            "value": 100
        }
        connector.add_timeseries_point("test_ns", "test_series", timestamp, old_data)
        
        # 添加较新的数据
        new_etl_time = (base_date + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        new_data = {
            "ds": "2024-01-01",
            "etl_time": new_etl_time,
            "value": 200
        }
        connector.add_timeseries_point("test_ns", "test_series", timestamp, new_data)
        
        # 测试 "none" 策略 - 应该保留所有数据
        variable_config_none = {
            "redis_config": {
                "prefix": [],
                "field": "test_series",
                "type": "timeseries",
                "drop_duplicate": "none"
            }
        }
        result_none = loader._load_timeseries(variable_config_none, "test_ns", {}, job_datetime=JOB_DATETIME)
        assert len(result_none) == 2  # 保留所有数据
        
        # 测试 "keep_latest" 策略 - 应该只保留最新的数据
        variable_config_latest = {
            "redis_config": {
                "prefix": [],
                "field": "test_series",
                "type": "timeseries",
                "drop_duplicate": "keep_latest"
            }
        }
        result_latest = loader._load_timeseries(variable_config_latest, "test_ns", {}, job_datetime=JOB_DATETIME)
        assert len(result_latest) == 1  # 只保留一条记录
        assert result_latest.iloc[0]["value"] == 200  # 保留最新的数据
        assert result_latest.iloc[0]["etl_time"] == new_etl_time  # 保留最新的etl_time
    
    def test_demo_integration(self, connector, loader):
        """测试完整的demo集成场景"""
        # 设置演示数据
        self.setup_demo_data(connector)
        
        # 获取配置
        data_config = self.get_demo_config()
        
        # 示例占位符值
        placeholders = {
            "cloud_nbr": "1001",
            "item_nbr": "2001",
            "biz_date": "2024-01-15"
        }
        
        # 作业日期时间
        job_datetime = "2024-01-15 10:30:00"
        
        # 加载数据
        data = loader.load_data(data_config, placeholders, job_datetime)

        print("data: ", data)
        print("job_datetime: ", job_datetime)
        print("placeholders: ", placeholders)
        print("data['df_quantity']: ", data['df_quantity'])
        print("data['city_cn']: ", data['city_cn'])
        print("data['same_city_forecast']: ", data['same_city_forecast'])
        
        # 验证结果
        assert len(data) == 3
        assert "df_quantity" in data
        assert "city_cn" in data
        assert "same_city_forecast" in data
        
        # 验证简单变量
        assert data["city_cn"] == "深圳"
        
        # 验证DataFrame
        assert isinstance(data["df_quantity"], pd.DataFrame)
        assert isinstance(data["same_city_forecast"], pd.DataFrame)
        
        # 验证DataFrame列结构
        if len(data["df_quantity"]) > 0:
            expected_columns = ["ds", "etl_time", "order_quantity", "is_activate"]
            assert all(col in data["df_quantity"].columns for col in expected_columns)
        
        if len(data["same_city_forecast"]) > 0:
            expected_columns = ["ds", "etl_time", "id1", "id2", "id3"]
            assert all(col in data["same_city_forecast"].columns for col in expected_columns)


if __name__ == "__main__":
    print("🧪 运行DataLoader测试")
    pytest.main([__file__, "-v"]) 