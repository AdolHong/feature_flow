#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能测试脚本：对比单次查询和批量查询的性能差异
"""

import time
import json
from data.redis_connector import RedisConnector
from data.dataloader import DataLoader
from typing import Dict


def create_test_data(redis_connector: RedisConnector, namespace: str, count: int = 1000):
    """
    创建测试数据
    
    Args:
        redis_connector: Redis连接器
        namespace: 命名空间
        count: 数据数量
    """
    print(f"🔄 正在创建 {count} 条测试数据...")
    
    # 创建value类型数据
    for i in range(count):
        key = f"test_value_{i}"
        value = f"value_{i}"
        redis_connector.store_direct_variable(namespace, key, value)
    
    # 创建json类型数据
    for i in range(count):
        key = f"test_json_{i}"
        value = {
            "id": i,
            "name": f"item_{i}",
            "data": {
                "score": i * 10,
                "status": "active" if i % 2 == 0 else "inactive"
            }
        }
        redis_connector.store_json_variable(namespace, key, value)
    
    print(f"✅ 测试数据创建完成，共 {count * 2} 条记录")


def create_test_config(count: int = 1000):
    """
    创建测试配置
    
    Args:
        count: 变量数量
        
    Returns:
        测试配置字典
    """
    variables = []
    
    # 添加value类型变量
    for i in range(count):
        variables.append({
            "name": f"value_var_{i}",
            "redis_config": {
                "type": "value",
                "field": f"test_value_{i}",
                "prefix": [
                    {"key": "env", "value": "prod"},
                    {"key": "region", "value": "cn"}
                ]
            }
        })
    
    # 添加json类型变量
    for i in range(count):
        variables.append({
            "name": f"json_var_{i}",
            "redis_config": {
                "type": "json",
                "field": f"test_json_{i}",
                "prefix": [
                    {"key": "env", "value": "prod"},
                    {"key": "region", "value": "cn"}
                ]
            }
        })
    
    return {
        "namespace": "test_performance",
        "placeholder": [
            {"name": "env", "value": "prod"},
            {"name": "region", "value": "cn"}
        ],
        "variable": variables
    }


def test_single_query(data_loader: DataLoader, data_config: Dict, placeholders: Dict, job_datetime: str, iterations: int = 10):
    """
    测试单次查询性能
    
    Args:
        data_loader: 数据加载器
        data_config: 数据配置
        placeholders: 占位符
        job_datetime: 作业时间
        iterations: 测试迭代次数
        
    Returns:
        平均耗时（秒）
    """
    print(f"🔄 开始单次查询性能测试，{iterations} 次迭代...")
    
    times = []
    for i in range(iterations):
        start_time = time.time()
        try:
            result = data_loader.load_data(data_config, placeholders, job_datetime)
            end_time = time.time()
            elapsed = end_time - start_time
            times.append(elapsed)
            print(f"  第 {i+1} 次: {elapsed:.4f} 秒")
        except Exception as e:
            print(f"  第 {i+1} 次失败: {e}")
    
    if times:
        avg_time = sum(times) / len(times)
        print(f"✅ 单次查询平均耗时: {avg_time:.4f} 秒")
        return avg_time
    else:
        print("❌ 单次查询测试失败")
        return None


def test_batch_query(data_loader: DataLoader, data_config: Dict, placeholders: Dict, job_datetime: str, iterations: int = 10):
    """
    测试批量查询性能
    
    Args:
        data_loader: 数据加载器
        data_config: 数据配置
        placeholders: 占位符
        job_datetime: 作业时间
        iterations: 测试迭代次数
        
    Returns:
        平均耗时（秒）
    """
    print(f"🔄 开始批量查询性能测试，{iterations} 次迭代...")
    
    times = []
    for i in range(iterations):
        start_time = time.time()
        try:
            result = data_loader.load_data_batch(data_config, placeholders, job_datetime)
            end_time = time.time()
            elapsed = end_time - start_time
            times.append(elapsed)
            print(f"  第 {i+1} 次: {elapsed:.4f} 秒")
        except Exception as e:
            print(f"  第 {i+1} 次失败: {e}")
    
    if times:
        avg_time = sum(times) / len(times)
        print(f"✅ 批量查询平均耗时: {avg_time:.4f} 秒")
        return avg_time
    else:
        print("❌ 批量查询测试失败")
        return None


def main():
    """主函数"""
    print("🚀 Redis批量查询性能测试")
    print("=" * 50)
    
    # 连接Redis
    try:
        redis_connector = RedisConnector()
        data_loader = DataLoader(redis_connector)
        print("✅ Redis连接成功")
    except Exception as e:
        print(f"❌ Redis连接失败: {e}")
        return
    
    # 测试参数
    test_counts = [10, 50, 100, 500, 1000]  # 测试不同数量的变量
    iterations = 5  # 每个测试的迭代次数
    
    # 测试配置
    placeholders = {"env": "prod", "region": "cn"}
    job_datetime = "2024-01-01 12:00:00"
    
    results = []
    
    for count in test_counts:
        print(f"\n📊 测试 {count} 个变量")
        print("-" * 30)
        
        # 创建测试数据
        create_test_data(redis_connector, "test_performance", count)
        
        # 创建测试配置
        data_config = create_test_config(count)
        
        # 测试单次查询
        single_time = test_single_query(data_loader, data_config, placeholders, job_datetime, iterations)
        
        # 测试批量查询
        batch_time = test_batch_query(data_loader, data_config, placeholders, job_datetime, iterations)
        
        # 计算性能提升
        if single_time and batch_time:
            improvement = (single_time - batch_time) / single_time * 100
            speedup = single_time / batch_time
            results.append({
                "count": count,
                "single_time": single_time,
                "batch_time": batch_time,
                "improvement": improvement,
                "speedup": speedup
            })
            
            print(f"📈 性能提升: {improvement:.2f}%")
            print(f"🚀 速度提升: {speedup:.2f}x")
        
        # 清理测试数据
        print("🧹 清理测试数据...")
        redis_connector.redis_client.delete(*redis_connector.redis_client.keys("test_performance::*"))
    
    # 输出总结
    print(f"\n📋 性能测试总结")
    print("=" * 50)
    print(f"{'变量数量':<8} {'单次查询(秒)':<12} {'批量查询(秒)':<12} {'性能提升(%)':<12} {'速度提升(x)':<12}")
    print("-" * 70)
    
    for result in results:
        print(f"{result['count']:<8} {result['single_time']:<12.4f} {result['batch_time']:<12.4f} {result['improvement']:<12.2f} {result['speedup']:<12.2f}")
    
    # 计算总体平均
    if results:
        avg_improvement = sum(r['improvement'] for r in results) / len(results)
        avg_speedup = sum(r['speedup'] for r in results) / len(results)
        print("-" * 70)
        print(f"{'平均':<8} {'':<12} {'':<12} {avg_improvement:<12.2f} {avg_speedup:<12.2f}")
        print(f"\n🎉 批量查询平均性能提升: {avg_improvement:.2f}%")
        print(f"🎉 批量查询平均速度提升: {avg_speedup:.2f}x")


if __name__ == "__main__":
    main() 