#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试densets数据类型功能
"""

import pandas as pd
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.redis_connector import RedisConnector
from data.dataloader import DataLoader
import json
from datetime import datetime
from utils.datetime_parser import parse_datetime_to_timestamp


def test_densets_functionality():
    """测试densets功能"""
    print("🧪 测试densets数据类型功能")
    print("=" * 50)
    
    # 连接Redis
    try:
        redis_connector = RedisConnector()
        data_loader = DataLoader(redis_connector)
        print("✅ Redis连接成功")
    except Exception as e:
        print(f"❌ Redis连接失败: {e}")
        return
    
    # 创建测试数据
    namespace = "test_densets"
    
    # 创建示例DataFrame数据
    test_data = [
        {"job_date": "2025-07-23", "fcst_date": "2025-07-23", "fcst_qty": 6.24, "etl_time": "2025-07-23 14:49:45"},
        {"job_date": "2025-07-23", "fcst_date": "2025-07-24", "fcst_qty": 6.44, "etl_time": "2025-07-23 14:49:45"},
        {"job_date": "2025-07-23", "fcst_date": "2025-07-25", "fcst_qty": 7.68, "etl_time": "2025-07-23 14:49:45"},
        {"job_date": "2025-07-23", "fcst_date": "2025-07-26", "fcst_qty": 8.21, "etl_time": "2025-07-23 14:49:45"},
        {"job_date": "2025-07-23", "fcst_date": "2025-07-27", "fcst_qty": 8.29, "etl_time": "2025-07-23 14:49:45"},
        {"job_date": "2025-07-23", "fcst_date": "2025-07-28", "fcst_qty": 6.6, "etl_time": "2025-07-23 14:49:45"},
        {"job_date": "2025-07-23", "fcst_date": "2025-07-29", "fcst_qty": 6.4, "etl_time": "2025-07-23 14:49:45"}
    ]
    
    df = pd.DataFrame(test_data)
    print("📊 原始DataFrame:")
    print(df)
    print()
    
    # 存储densets数据
    print("💾 存储densets数据...")
    series_key = "forecast_data"
    base_timestamp = parse_datetime_to_timestamp("2025-07-23 14:49:45")
    
    # 使用正确的键名格式
    redis_key = f"{namespace}::densets::env=prod::region=cn::{series_key}"
    
    # 定义schema
    schema = "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string"
    
    for i, row in df.iterrows():
        # 将行数据转换为字典
        row_dict = row.to_dict()
        
        # 存储为densets格式，使用schema保证字段顺序
        timestamp = base_timestamp + i * 86400  # 每天递增
        redis_connector.add_densets_point(
            namespace, 
            f"env=prod::region=cn::{series_key}", 
            timestamp, 
            row_dict,
            schema=schema
        )
    
    print("✅ densets数据存储完成")
    print()
    
    # 测试数据加载
    print("📥 测试数据加载...")
    
    # 创建数据配置
    data_config = {
        "namespace": namespace,
        "placeholder": [
            {"name": "env", "value": "prod"},
            {"name": "region", "value": "cn"}
        ],
        "variable": [
            {
                "name": "forecast_data",
                "redis_config": {
                    "type": "densets",
                    "field": "forecast_data",
                    "prefix": [
                        {"key": "env", "value": "${env}"},
                        {"key": "region", "value": "${region}"}
                    ],
                    "split": ",",
                    "schema": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string",
                    "drop_duplicate": "none"
                }
            }
        ]
    }
    
    placeholders = {"env": "prod", "region": "cn"}
    job_datetime = "2025-07-23 14:49:45"
    
    # 加载数据
    try:
        loaded_data = data_loader.load_data(data_config, placeholders, job_datetime)
        print("✅ 数据加载成功")
        
        # 显示加载的数据
        if "forecast_data" in loaded_data:
            df_loaded = loaded_data["forecast_data"]
            print("📊 加载的DataFrame:")
            print(df_loaded)
            print()
            
            print("📋 DataFrame信息:")
            print(f"形状: {df_loaded.shape}")
            print(f"列名: {list(df_loaded.columns)}")
            print()
            
            # 显示前几行数据
            if len(df_loaded) > 0:
                print("📄 前3行数据:")
                print(df_loaded.head(3))
        else:
            print("❌ 未找到forecast_data变量")
            
    except Exception as e:
        print(f"❌ 数据加载失败: {e}")
    
    # 清理测试数据
    print("\n🧹 清理测试数据...")
    redis_connector.redis_client.delete(f"{namespace}::densets::env=prod::region=cn::{series_key}")
    print("✅ 测试数据已清理")


def test_densets_without_columns():
    """测试不带列名配置的densets功能"""
    print("\n🧪 测试不带列名配置的densets功能")
    print("=" * 50)
    
    # 连接Redis
    try:
        redis_connector = RedisConnector()
        data_loader = DataLoader(redis_connector)
        print("✅ Redis连接成功")
    except Exception as e:
        print(f"❌ Redis连接失败: {e}")
        return
    
    # 创建测试数据
    namespace = "test_densets_simple"
    
    # 创建简单的测试数据
    test_data = [
        {"name": "张三", "age": 30, "city": "北京"},
        {"name": "李四", "age": 25, "city": "上海"},
        {"name": "王五", "age": 35, "city": "广州"}
    ]
    
    df = pd.DataFrame(test_data)
    print("📊 原始DataFrame:")
    print(df)
    print()
    
    # 存储densets数据
    print("💾 存储densets数据...")
    series_key = "user_info"
    base_timestamp = parse_datetime_to_timestamp("2025-07-23 14:49:45")
    
    for i, row in df.iterrows():
        row_dict = row.to_dict()
        timestamp = base_timestamp + i * 3600  # 每小时递增
        redis_connector.add_densets_point(namespace, series_key, timestamp, row_dict)
    
    print("✅ densets数据存储完成")
    print()
    
    # 测试数据加载（不指定列名）
    print("📥 测试数据加载（不指定列名）...")
    
    data_config = {
        "namespace": namespace,
        "variable": [
            {
                "name": "user_info",
                "redis_config": {
                    "type": "densets",
                    "field": "user_info",
                    "split": ",",
                    "drop_duplicate": "none"
                }
            }
        ]
    }
    
    placeholders = {}
    job_datetime = "2025-07-23 14:49:45"
    
    try:
        loaded_data = data_loader.load_data(data_config, placeholders, job_datetime)
        print("✅ 数据加载成功")
        
        if "user_info" in loaded_data:
            df_loaded = loaded_data["user_info"]
            print("📊 加载的DataFrame（无列名配置）:")
            print(df_loaded)
            print()
            
            print("📋 DataFrame信息:")
            print(f"形状: {df_loaded.shape}")
            print(f"列名: {list(df_loaded.columns)}")
            
    except Exception as e:
        print(f"❌ 数据加载失败: {e}")
    
    # 清理测试数据
    print("\n🧹 清理测试数据...")
    redis_connector.redis_client.delete(f"{namespace}::densets::{series_key}")
    print("✅ 测试数据已清理")


def test_densets_schema_validation():
    """测试densets的schema验证功能"""
    print("\n🧪 测试densets的schema验证功能")
    print("=" * 50)
    
    # 连接Redis
    try:
        redis_connector = RedisConnector()
        data_loader = DataLoader(redis_connector)
        print("✅ Redis连接成功")
    except Exception as e:
        print(f"❌ Redis连接失败: {e}")
        return
    
    # 创建测试数据
    namespace = "test_densets_schema"
    
    # 创建包含不同类型数据的测试数据
    test_data = [
        {"job_date": "2025-07-23", "fcst_date": "2025-07-23", "fcst_qty": 6.24, "etl_time": "2025-07-23 14:49:45", "is_active": True},
        {"job_date": "2025-07-23", "fcst_date": "2025-07-24", "fcst_qty": 6.44, "etl_time": "2025-07-23 14:49:45", "is_active": False},
        {"job_date": "2025-07-23", "fcst_date": "2025-07-25", "fcst_qty": 7.68, "etl_time": "2025-07-23 14:49:45", "is_active": True}
    ]
    
    df = pd.DataFrame(test_data)
    print("📊 原始DataFrame:")
    print(df)
    print()
    
    # 存储densets数据
    print("💾 存储densets数据...")
    series_key = "forecast_data_with_types"
    base_timestamp = parse_datetime_to_timestamp("2025-07-23 14:49:45")
    
    for i, row in df.iterrows():
        row_dict = row.to_dict()
        timestamp = base_timestamp + i * 86400
        redis_connector.add_densets_point(namespace, series_key, timestamp, row_dict)
    
    print("✅ densets数据存储完成")
    print()
    
    # 测试1: 正确的schema配置
    print("📥 测试1: 正确的schema配置...")
    data_config = {
        "namespace": namespace,
        "variable": [
            {
                "name": "forecast_data",
                "redis_config": {
                    "type": "densets",
                    "field": "forecast_data_with_types",
                    "split": ",",
                    "schema": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string,is_active:boolean",
                    "drop_duplicate": "none"
                }
            }
        ]
    }
    
    placeholders = {}
    job_datetime = "2025-07-23 14:49:45"
    
    try:
        loaded_data = data_loader.load_data(data_config, placeholders, job_datetime)
        print("✅ 数据加载成功")
        
        if "forecast_data" in loaded_data:
            df_loaded = loaded_data["forecast_data"]
            print("📊 加载的DataFrame:")
            print(df_loaded)
            print()
            
            print("📋 DataFrame信息:")
            print(f"形状: {df_loaded.shape}")
            print(f"列名: {list(df_loaded.columns)}")
            print(f"数据类型: {df_loaded.dtypes.to_dict()}")
            
    except Exception as e:
        print(f"❌ 数据加载失败: {e}")
    
    # 测试2: 值数量不足的情况
    print("\n📥 测试2: 值数量不足的情况...")
    data_config_invalid = {
        "namespace": namespace,
        "variable": [
            {
                "name": "forecast_data",
                "redis_config": {
                    "type": "densets",
                    "field": "forecast_data_with_types",
                    "split": ",",
                    "schema": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string,is_active:boolean,extra_field:string",
                    "drop_duplicate": "none"
                }
            }
        ]
    }
    
    loaded_data = data_loader.load_data(data_config_invalid, placeholders, job_datetime)
    if "forecast_data" in loaded_data and loaded_data["forecast_data"] is None:
        print("✅ 正确捕获数量不足错误，变量值为None")
    else:
        print("❌ 应该报错但没有报错")
    
    # 测试3: 类型转换错误的情况
    print("\n📥 测试3: 类型转换错误的情况...")
    # 存储一些错误类型的数据
    error_data = [
        {"job_date": "2025-07-23", "fcst_date": "2025-07-23", "fcst_qty": "not_a_number", "etl_time": "2025-07-23 14:49:45"},
    ]
    
    error_series_key = "error_data"
    for i, row in enumerate(error_data):
        row_dict = row
        timestamp = base_timestamp + i * 86400
        redis_connector.add_densets_point(namespace, error_series_key, timestamp, row_dict)
    
    data_config_error = {
        "namespace": namespace,
        "variable": [
            {
                "name": "error_data",
                "redis_config": {
                    "type": "densets",
                    "field": "error_data",
                    "split": ",",
                    "schema": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string",
                    "drop_duplicate": "none"
                }
            }
        ]
    }
    
    loaded_data = data_loader.load_data(data_config_error, placeholders, job_datetime)
    if "error_data" in loaded_data and loaded_data["error_data"] is None:
        print("✅ 正确捕获类型转换错误，变量值为None")
    else:
        print("❌ 应该报错但没有报错")
    
    # 清理测试数据
    print("\n🧹 清理测试数据...")
    redis_connector.redis_client.delete(f"{namespace}::densets::{series_key}")
    redis_connector.redis_client.delete(f"{namespace}::densets::{error_series_key}")
    print("✅ 测试数据已清理")


def test_densets_schema_order():
    """测试densets schema字段顺序功能"""
    print("\n🧪 测试densets schema字段顺序功能")
    print("=" * 50)
    
    # 连接Redis
    try:
        redis_connector = RedisConnector()
        print("✅ Redis连接成功")
    except Exception as e:
        print(f"❌ Redis连接失败: {e}")
        return
    
    # 创建测试数据
    namespace = "test_densets_order"
    series_key = "order_test"
    
    # 测试数据 - 字段顺序混乱
    test_data = [
        {"c": 3, "a": 1, "b": 2, "d": 4},
        {"d": 8, "b": 6, "a": 5, "c": 7},
    ]
    
    # 定义schema - 固定字段顺序
    schema = "a:int,b:int,c:int,d:int"
    
    print(f"📋 使用schema: {schema}")
    print("📊 测试数据:")
    for i, data in enumerate(test_data):
        print(f"  数据{i+1}: {data}")
    print()
    
    # 存储数据
    print("💾 存储densets数据...")
    base_timestamp = parse_datetime_to_timestamp("2025-07-23 14:49:45")
    
    for i, data in enumerate(test_data):
        timestamp = base_timestamp + i * 86400
        redis_connector.add_densets_point(
            namespace, 
            series_key, 
            timestamp, 
            data,
            schema=schema
        )
    
    print("✅ 数据存储完成")
    print()
    
    # 直接获取Redis数据验证顺序
    print("🔍 验证Redis中存储的数据顺序...")
    redis_key = f"{namespace}::densets::{series_key}"
    results = redis_connector.redis_client.zrange(redis_key, 0, -1, withscores=True)
    
    for i, (value_str, timestamp) in enumerate(results):
        print(f"  数据{i+1}: {value_str}")
        values = value_str.split(',')
        print(f"    解析后: {values}")
        print(f"    期望顺序: [1, 2, 3, 4]" if i == 0 else "    期望顺序: [5, 6, 7, 8]")
        print()
    
    # 清理测试数据
    print("🧹 清理测试数据...")
    redis_connector.redis_client.delete(redis_key)
    print("✅ 测试数据已清理")


if __name__ == "__main__":
    test_densets_functionality()
    test_densets_without_columns()
    test_densets_schema_validation()
    test_densets_schema_order() 