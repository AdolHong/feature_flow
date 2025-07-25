 #!/usr/bin/env python3
"""
测试 DataLoader 中 densets 的批量加载功能
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


class TestDataLoaderDensetsBatch:
    """DataLoader densets 批量加载测试类"""
    
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
    
    def setup_densets_data(self, connector: RedisConnector):
        """设置 densets 演示数据"""
        print("🔧 设置 densets 演示数据...")
        
        # 清空现有数据
        connector.clear_all()
        
        # 1. 设置简单变量数据
        connector.store_direct_variable("test_densets", "cloud_nbr=1001::city_cn", "深圳")
        
        # 2. 设置JSON变量数据
        store_info = {
            "mother_store_nbr": "1001",
            "city_en": "Shenzhen",
            "city_cn": "深圳"
        }
        connector.store_json_variable("test_densets", "cloud_nbr=1001::dim_store", store_info)
        
        # 3. 设置 densets 数据
        base_time = datetime(2024, 1, 1)
        for i in range(5):
            current_date = base_time + timedelta(days=i)
            timestamp = current_date.timestamp()
            
            # 使用 schema 格式的数据
            row_data = f"2024-01-{i+1:02d},2024-01-{i+1:02d},{100 + i * 10:.1f},2024-01-{i+1:02d} 10:00:00"
            
            connector.add_densets_point(
                "test_densets",
                "cloud_nbr=1001::item_nbr=2001::fcst_data",
                timestamp,
                row_data
            )
        
        # 4. 设置另一个 densets 数据（不带 schema）
        for i in range(3):
            current_date = base_time + timedelta(days=i)
            timestamp = current_date.timestamp()
            
            # 简单的逗号分隔数据
            row_data = f"value1_{i},value2_{i},{50.5 + i * 5.2}"
            
            connector.add_densets_point(
                "test_densets",
                "cloud_nbr=1001::simple_data",
                timestamp,
                row_data
            )
        
        print("✅ densets 演示数据设置完成")
    
    def get_densets_config(self):
        """获取 densets 演示配置"""
        return {
            "namespace": "test_densets",
            "placeholder": [
                {"name": "cloud_nbr", "alias": None, "description": "云仓号"},
                {"name": "item_nbr", "alias": None, "description": "商品号"},
            ],
            "variable": [
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
                    "name": "dim_store",
                    "alias": None,
                    "description": None,
                    "namespace": None,
                    "redis_config": {
                        "prefix": [
                            {"key": "cloud_nbr", "value": "${cloud_nbr}"}
                        ],
                        "field": "dim_store",
                        "type": "json"
                    }
                },
                {
                    "name": "fcst_data",
                    "alias": None,
                    "description": None,
                    "namespace": None,
                    "redis_config": {
                        "prefix": [
                            {"key": "cloud_nbr", "value": "${cloud_nbr}"},
                            {"key": "item_nbr", "value": "${item_nbr}"}
                        ],
                        "field": "fcst_data",
                        "type": "densets",
                        "from_datetime": "${yyyy-MM-dd-5d}",
                        "to_datetime": "${yyyy-MM-dd+5d}",
                        "drop_duplicate": "none",
                        "split": ",",
                        "schema": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string"
                    }
                },
                {
                    "name": "simple_data",
                    "alias": None,
                    "description": None,
                    "namespace": None,
                    "redis_config": {
                        "prefix": [
                            {"key": "cloud_nbr", "value": "${cloud_nbr}"}
                        ],
                        "field": "simple_data",
                        "type": "densets",
                        "from_datetime": "${yyyy-MM-dd-3d}",
                        "to_datetime": "${yyyy-MM-dd+3d}",
                        "drop_duplicate": "none",
                        "split": ","
                    }
                }
            ]
        }
    
    def test_densets_batch_loading(self, connector, loader):
        """测试 densets 批量加载功能"""
        # 设置演示数据
        self.setup_densets_data(connector)
        
        # 获取配置
        data_config = self.get_densets_config()
        
        # 示例占位符值
        placeholders = {
            "cloud_nbr": "1001",
            "item_nbr": "2001"
        }
        
        # 作业日期时间
        job_datetime = "2024-01-15 10:30:00"
        
        # 测试单次加载
        print("\n🔄 测试单次加载...")
        single_result = loader.load_data(data_config, placeholders, job_datetime)
        
        # 测试批量加载
        print("\n🔄 测试批量加载...")
        batch_result = loader.load_data_batch(data_config, placeholders, job_datetime)
        
        # 验证结果一致性
        assert len(single_result) == len(batch_result)
        assert set(single_result.keys()) == set(batch_result.keys())
        
        # 验证简单变量
        assert single_result["city_cn"] == batch_result["city_cn"] == "深圳"
        
        # 验证 JSON 变量
        expected_store_info = {
            "mother_store_nbr": "1001",
            "city_en": "Shenzhen",
            "city_cn": "深圳"
        }
        assert single_result["dim_store"] == batch_result["dim_store"] == expected_store_info
        
        # 验证 densets 数据
        assert isinstance(single_result["fcst_data"], pd.DataFrame)
        assert isinstance(batch_result["fcst_data"], pd.DataFrame)
        assert len(single_result["fcst_data"]) == len(batch_result["fcst_data"])
        
        # 验证带 schema 的 densets 数据列
        if len(single_result["fcst_data"]) > 0:
            expected_columns = ["job_date", "fcst_date", "fcst_qty", "etl_time"]
            assert all(col in single_result["fcst_data"].columns for col in expected_columns)
            assert all(col in batch_result["fcst_data"].columns for col in expected_columns)
            
            # 验证数据类型
            assert single_result["fcst_data"]["fcst_qty"].dtype in ['float64', 'float32']
            assert batch_result["fcst_data"]["fcst_qty"].dtype in ['float64', 'float32']
        
        # 验证不带 schema 的 densets 数据
        assert isinstance(single_result["simple_data"], pd.DataFrame)
        assert isinstance(batch_result["simple_data"], pd.DataFrame)
        assert len(single_result["simple_data"]) == len(batch_result["simple_data"])
        
        if len(single_result["simple_data"]) > 0:
            # 不带 schema 时应该有 __values__ 列
            assert "__values__" in single_result["simple_data"].columns
            assert "__values__" in batch_result["simple_data"].columns
        
        print("✅ densets 批量加载测试通过")
    
    def test_densets_with_timeseries_mixed(self, connector, loader):
        """测试 densets 和时间序列混合的批量加载"""
        # 设置混合数据
        connector.clear_all()
        
        # 设置简单变量
        connector.store_direct_variable("test_mixed", "cloud_nbr=1001::city", "北京")
        
        # 设置 densets 数据
        base_time = datetime(2024, 1, 1)
        for i in range(3):
            current_date = base_time + timedelta(days=i)
            timestamp = current_date.timestamp()
            row_data = f"2024-01-{i+1:02d},{100 + i * 10:.1f}"
            connector.add_densets_point("test_mixed", "cloud_nbr=1001::densets_data", timestamp, row_data)
        
        # 设置时间序列数据
        for i in range(3):
            current_date = base_time + timedelta(days=i)
            ds = current_date.strftime("%Y-%m-%d")
            etl_time = (current_date + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            data = {
                "ds": ds,
                "etl_time": etl_time,
                "quantity": 200 + i * 20
            }
            connector.add_timeseries_point("test_mixed", "cloud_nbr=1001::timeseries_data", current_date.timestamp(), data)
        
        # 配置
        data_config = {
            "namespace": "test_mixed",
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
                    "name": "densets_data",
                    "redis_config": {
                        "prefix": [{"key": "cloud_nbr", "value": "${cloud_nbr}"}],
                        "field": "densets_data",
                        "type": "densets",
                        "split": ",",
                        "schema": "date:string,value:double"
                    }
                },
                {
                    "name": "timeseries_data",
                    "redis_config": {
                        "prefix": [{"key": "cloud_nbr", "value": "${cloud_nbr}"}],
                        "field": "timeseries_data",
                        "type": "timeseries"
                    }
                }
            ]
        }
        
        placeholders = {"cloud_nbr": "1001"}
        job_datetime = "2024-01-15 10:30:00"
        
        # 测试批量加载
        batch_result = loader.load_data_batch(data_config, placeholders, job_datetime)
        
        # 验证结果
        assert len(batch_result) == 3
        assert "city" in batch_result
        assert "densets_data" in batch_result
        assert "timeseries_data" in batch_result
        
        # 验证简单变量
        assert batch_result["city"] == "北京"
        
        # 验证 densets 数据
        assert isinstance(batch_result["densets_data"], pd.DataFrame)
        assert len(batch_result["densets_data"]) == 3
        assert "date" in batch_result["densets_data"].columns
        assert "value" in batch_result["densets_data"].columns
        
        # 验证时间序列数据
        assert isinstance(batch_result["timeseries_data"], pd.DataFrame)
        assert len(batch_result["timeseries_data"]) == 3
        assert "ds" in batch_result["timeseries_data"].columns
        assert "etl_time" in batch_result["timeseries_data"].columns
        assert "quantity" in batch_result["timeseries_data"].columns
        
        print("✅ 混合数据类型批量加载测试通过")
    
    def test_densets_deduplication_in_batch(self, connector, loader):
        """测试 densets 在批量加载中的去重功能"""
        # 设置重复数据
        connector.clear_all()
        
        base_time = datetime(2024, 1, 1)
        timestamp = base_time.timestamp()
        
        # 添加重复的 densets 数据
        row_data1 = "2024-01-01,100.5,2024-01-01 09:00:00"
        row_data2 = "2024-01-01,200.5,2024-01-01 10:00:00"  # 更新的数据
        
        connector.add_densets_point("test_dedup", "test_series", timestamp, row_data1)
        connector.add_densets_point("test_dedup", "test_series", timestamp, row_data2)
        
        # 配置
        data_config = {
            "namespace": "test_dedup",
            "placeholder": [],
            "variable": [
                {
                    "name": "test_data",
                    "redis_config": {
                        "prefix": [],
                        "field": "test_series",
                        "type": "densets",
                        "split": ",",
                        "schema": "date:string,value:double,etl_time:string",
                        "drop_duplicate": "keep_latest"
                    }
                }
            ]
        }
        
        placeholders = {}
        job_datetime = "2024-01-15 10:30:00"
        
        # 测试批量加载
        batch_result = loader.load_data_batch(data_config, placeholders, job_datetime)
        
        # 验证去重效果
        assert len(batch_result["test_data"]) == 1
        # 在 densets 中，后添加的数据会覆盖先添加的数据，所以应该保留后添加的数据
        assert batch_result["test_data"].iloc[0]["value"] == 200.5
        assert batch_result["test_data"].iloc[0]["etl_time"] == "2024-01-01 10:00:00"
        
        print("✅ densets 去重功能测试通过")

    def test_densets_load_and_batch_consistency(self, connector, loader):
        """测试 densets 用 load_data 和 load_data_batch 读取一致性"""
        connector.clear_all()
        namespace = "test_consistency"
        series_key = "cloud_nbr=1001::item_nbr=2001::fcst_data"
        schema = "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string"
        base_time = datetime(2024, 1, 1)
        # 构造3条数据
        for i in range(3):
            current_date = base_time + timedelta(days=i)
            timestamp = current_date.timestamp()
            row_data = f"2024-01-{i+1:02d},2024-01-{i+1:02d},{100 + i * 10:.1f},2024-01-{i+1:02d} 10:00:00"
            connector.add_densets_point(namespace, series_key, timestamp, row_data)
        # 配置
        data_config = {
            "namespace": namespace,
            "placeholder": [
                {"name": "cloud_nbr", "alias": None, "description": "云仓号"},
                {"name": "item_nbr", "alias": None, "description": "商品号"},
            ],
            "variable": [
                {
                    "name": "fcst_data",
                    "redis_config": {
                        "prefix": [
                            {"key": "cloud_nbr", "value": "${cloud_nbr}"},
                            {"key": "item_nbr", "value": "${item_nbr}"}
                        ],
                        "field": "fcst_data",
                        "type": "densets",
                        "from_datetime": "${yyyy-MM-dd-5d}",
                        "to_datetime": "${yyyy-MM-dd+5d}",
                        "drop_duplicate": "none",
                        "split": ",",
                        "schema": schema
                    }
                }
            ]
        }
        placeholders = {"cloud_nbr": "1001", "item_nbr": "2001"}
        job_datetime = "2024-01-15 10:30:00"
        # 分别读取
        result1 = loader.load_data(data_config, placeholders, job_datetime)
        result2 = loader.load_data_batch(data_config, placeholders, job_datetime)
        # 检查key
        assert "fcst_data" in result1 and "fcst_data" in result2
        # 检查类型
        assert isinstance(result1["fcst_data"], pd.DataFrame)
        assert isinstance(result2["fcst_data"], pd.DataFrame)
        # 检查内容完全一致
        pd.testing.assert_frame_equal(result1["fcst_data"], result2["fcst_data"])
        print("✅ load_data 和 load_data_batch densets 读取一致性测试通过")


if __name__ == "__main__":
    print("🧪 运行 DataLoader densets 批量加载测试")
    pytest.main([__file__, "-v"])