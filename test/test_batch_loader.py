#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试批量加载功能
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.redis_connector import RedisConnector
from data.dataloader import DataLoader
import json


def test_batch_loader():
    """测试批量加载功能"""
    print("🧪 测试批量加载功能")
    print("=" * 40)
    
    # 连接Redis
    try:
        redis_connector = RedisConnector()
        data_loader = DataLoader(redis_connector)
        print("✅ Redis连接成功")
    except Exception as e:
        print(f"❌ Redis连接失败: {e}")
        return
    
    # 创建测试数据
    namespace = "test_batch"
    
    # 存储一些测试数据（按照正确的键名格式）
    print("📝 创建测试数据...")
    
    # 直接存储到Redis，使用正确的键名格式
    redis_connector.redis_client.set("test_batch::value::env=test::region=cn::user_count", "1000")
    redis_connector.redis_client.set("test_batch::value::env=test::region=cn::order_count", "5000")
    redis_connector.redis_client.set("test_batch::json::env=test::region=cn::user_info", json.dumps({
        "name": "张三",
        "age": 30,
        "city": "北京"
    }))
    redis_connector.redis_client.set("test_batch::json::env=test::region=cn::order_info", json.dumps({
        "order_id": "ORD001",
        "amount": 299.99,
        "status": "completed"
    }))
    
    print("✅ 测试数据创建完成")
    
    # 创建测试配置
    data_config = {
        "namespace": namespace,
        "placeholder": [
            {"name": "env", "value": "test"},
            {"name": "region", "value": "cn"}
        ],
        "variable": [
            {
                "name": "user_count",
                "redis_config": {
                    "type": "value",
                    "field": "user_count",
                    "prefix": [
                        {"key": "env", "value": "${env}"},
                        {"key": "region", "value": "${region}"}
                    ]
                }
            },
            {
                "name": "order_count",
                "redis_config": {
                    "type": "value",
                    "field": "order_count",
                    "prefix": [
                        {"key": "env", "value": "${env}"},
                        {"key": "region", "value": "${region}"}
                    ]
                }
            },
            {
                "name": "user_info",
                "redis_config": {
                    "type": "json",
                    "field": "user_info",
                    "prefix": [
                        {"key": "env", "value": "${env}"},
                        {"key": "region", "value": "${region}"}
                    ]
                }
            },
            {
                "name": "order_info",
                "redis_config": {
                    "type": "json",
                    "field": "order_info",
                    "prefix": [
                        {"key": "env", "value": "${env}"},
                        {"key": "region", "value": "${region}"}
                    ]
                }
            }
        ]
    }
    
    placeholders = {"env": "test", "region": "cn"}
    job_datetime = "2024-01-01 12:00:00"
    
    # 测试单次加载
    print("\n🔄 测试单次加载...")
    try:
        single_result = data_loader.load_data(data_config, placeholders, job_datetime)
        print("✅ 单次加载成功")
        print(f"   结果: {json.dumps(single_result, ensure_ascii=False, indent=2)}")
    except Exception as e:
        print(f"❌ 单次加载失败: {e}")
    
    # 测试批量加载
    print("\n🔄 测试批量加载...")
    try:
        batch_result = data_loader.load_data_batch(data_config, placeholders, job_datetime)
        print("✅ 批量加载成功")
        print(f"   结果: {json.dumps(batch_result, ensure_ascii=False, indent=2)}")
    except Exception as e:
        print(f"❌ 批量加载失败: {e}")
    
    # 验证结果一致性
    if 'single_result' in locals() and 'batch_result' in locals():
        if single_result == batch_result:
            print("\n✅ 单次加载和批量加载结果一致")
        else:
            print("\n❌ 单次加载和批量加载结果不一致")
            print(f"单次加载: {single_result}")
            print(f"批量加载: {batch_result}")
    
    # 清理测试数据
    print("\n🧹 清理测试数据...")
    redis_connector.redis_client.delete(*redis_connector.redis_client.keys(f"{namespace}::*"))
    print("✅ 测试完成")


if __name__ == "__main__":
    test_batch_loader() 