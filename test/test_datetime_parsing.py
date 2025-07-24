#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试日期时间解析功能
验证DataLoader中的日期时间解析是否正确工作
"""

from utils.datetime_parser import parse_datetime
from data.dataloader import DataLoader
from data.redis_connector import RedisConnector


def test_datetime_parsing():
    """测试日期时间解析功能"""
    print("🧪 测试日期时间解析功能")
    print("=" * 50)
    
    # 测试各种日期时间模式
    test_cases = [
        "${yyyy-MM-dd-60d}",
        "${yyyy-MM-dd-1d}",
        "${yyyy-MM-dd}",
        "${yyyy-MM-dd+13d}",
        "job_date=${yyyy-MM-dd}",
        "from=${yyyy-MM-dd-60d}&to=${yyyy-MM-dd-1d}"
    ]
    
    job_datetime = "2024-01-15 10:30:00"
    print(f"📅 作业日期时间: {job_datetime}")
    print()
    
    for i, pattern in enumerate(test_cases, 1):
        try:
            result, timestamp = parse_datetime(pattern, pattern)
            print(f"测试 {i:2d}: {pattern}")
            print(f"结果: {result}")
            print(f"时间戳: {timestamp}")
            print()
        except Exception as e:
            print(f"测试 {i:2d}: {pattern}")
            print(f"错误: {e}")
            print()


def test_dataloader_datetime():
    """测试DataLoader中的日期时间解析"""
    print("🧪 测试DataLoader中的日期时间解析")
    print("=" * 50)
    
    # 简化的配置，只测试日期时间解析
    data_config = {
        "namespace": "test_namespace",
        "placeholder": [
            {"name": "cloud_nbr", "alias": None, "description": "云仓号"},
        ],
        "variable": [
            {
                "name": "test_timeseries",
                "alias": None,
                "description": None,
                "namespace": None,
                "type": "homogeneous_timeseries",
                "type_config": {
                    "type": "double"
                },
                "redis_config": {
                    "prefix": [
                        {"key": "cloud_nbr", "value": "${cloud_nbr}"},
                        {"key": "job_date", "value": "${yyyy-MM-dd}"}
                    ],
                    "field": "test_field",
                    "type": "timeseries",
                    "from_datetime": "${yyyy-MM-dd-7d}",
                    "to_datetime": "${yyyy-MM-dd+7d}",
                }
            }
        ]
    }
    
    placeholders = {
        "cloud_nbr": "TEST001"
    }
    
    job_datetime = "2024-01-15 10:30:00"
    
    try:
        # 创建DataLoader实例
        connector = RedisConnector()
        loader = DataLoader(connector)
        
        # 测试占位符替换
        print("📝 测试占位符替换...")
        template = "${cloud_nbr}::job_date=${yyyy-MM-dd}"
        result = loader._replace_placeholders(template, placeholders, job_datetime)
        print(f"模板: {template}")
        print(f"结果: {result}")
        print()
        
        # 测试Redis键构建
        print("📝 测试Redis键构建...")
        redis_config = data_config["variable"][0]["redis_config"]
        redis_key = loader._build_redis_key(
            data_config["namespace"],
            "timeseries",
            redis_config["prefix"],
            redis_config["field"],
            placeholders,
            job_datetime
        )
        print(f"构建的Redis键: {redis_key}")
        print()
        
        # 测试时间戳解析
        print("📝 测试时间戳解析...")
        from_datetime = redis_config["from_datetime"]
        to_datetime = redis_config["to_datetime"]
        
        _, from_timestamp = parse_datetime(from_datetime, from_datetime)
        _, to_timestamp = parse_datetime(to_datetime, to_datetime)
        
        print(f"from_datetime: {from_datetime}")
        print(f"from_timestamp: {from_timestamp}")
        print(f"to_datetime: {to_datetime}")
        print(f"to_timestamp: {to_timestamp}")
        print()
        
        print("✅ 日期时间解析测试完成！")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_datetime_parsing()
    print("\n" + "=" * 50 + "\n")
    test_dataloader_datetime() 