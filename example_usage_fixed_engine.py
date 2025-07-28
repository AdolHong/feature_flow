#!/usr/bin/env python3
"""
使用修复后的引擎字符串的示例
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fixed_engine_string import get_engine_string, FIXED_JSON5_ENGINE, STANDARD_JSON_ENGINE

def execute_engine(job_datetime, placeholders, loaded_data, encoded_engine):
    """
    执行引擎的函数
    
    Args:
        job_datetime (str): 作业日期时间
        placeholders (dict): 占位符数据
        loaded_data (dict): 加载的数据
        encoded_engine (str): 引擎配置字符串
    
    Returns:
        tuple: (engine, results)
    """
    try:
        from core.engine import RuleEngine
        
        # 创建引擎
        engine = RuleEngine.import_from_json(encoded_engine)
        
        # 合并数据
        execution_data = {}
        if placeholders:
            execution_data.update(placeholders)
        if loaded_data:
            execution_data.update(loaded_data)
        
        # 执行引擎
        results = engine.execute(
            job_date=job_datetime,
            placeholders=execution_data
        )
        
        return engine, results
        
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        raise Exception(f"执行失败: {e}")

def main():
    """主函数"""
    print("=== 使用修复后的引擎字符串示例 ===")
    
    # 模拟数据
    job_datetime = "2024-01-15"
    placeholders = {
        "store_nbr": "001",
        "item_nbr": "12345",
        "dim_store": {"city_cn": "北京"},
        "dim_item": {"category_nbr": "1", "is_fresh": "0"},
        "xgb_forecast": {"fcst_date": "2024-01-16", "fcst_qty": 100},
        "tft_forecast": {"fcst_date": "2024-01-16", "fcst_qty": 110},
        "fence_forecast": {"fcst_date": "2024-01-16", "fcst_qty": 105},
        "prophet_forecast": {"fcst_date": "2024-01-16", "fcst_qty": 95}
    }
    loaded_data = {}
    
    # 方法1: 使用JSON5格式
    print("\n1. 使用JSON5格式:")
    try:
        engine, results = execute_engine(job_datetime, placeholders, loaded_data, FIXED_JSON5_ENGINE)
        print("✅ JSON5格式执行成功!")
        
        # 显示结果
        for node_name, result in results.items():
            if node_name == "start_node":
                continue
            print(f"  {node_name}: {'✅' if result.success else '❌'}")
            if not result.success:
                print(f"    错误: {result.error}")
                
    except Exception as e:
        print(f"❌ JSON5格式执行失败: {e}")
    
    # 方法2: 使用标准JSON格式
    print("\n2. 使用标准JSON格式:")
    try:
        engine, results = execute_engine(job_datetime, placeholders, loaded_data, STANDARD_JSON_ENGINE)
        print("✅ 标准JSON格式执行成功!")
        
        # 显示结果
        for node_name, result in results.items():
            if node_name == "start_node":
                continue
            print(f"  {node_name}: {'✅' if result.success else '❌'}")
            if not result.success:
                print(f"    错误: {result.error}")
                
    except Exception as e:
        print(f"❌ 标准JSON格式执行失败: {e}")
    
    # 方法3: 使用函数获取引擎字符串
    print("\n3. 使用函数获取引擎字符串:")
    try:
        # 获取JSON5格式
        json5_engine = get_engine_string(use_json5=True)
        engine, results = execute_engine(job_datetime, placeholders, loaded_data, json5_engine)
        print("✅ 函数获取JSON5格式执行成功!")
        
        # 获取标准JSON格式
        json_engine = get_engine_string(use_json5=False)
        engine, results = execute_engine(job_datetime, placeholders, loaded_data, json_engine)
        print("✅ 函数获取标准JSON格式执行成功!")
        
    except Exception as e:
        print(f"❌ 函数获取引擎字符串执行失败: {e}")

def show_usage_example():
    """显示使用示例"""
    print("\n=== 使用示例 ===")
    print("""
# 方法1: 直接使用修复后的字符串
from fixed_engine_string import FIXED_JSON5_ENGINE, STANDARD_JSON_ENGINE

# 使用JSON5格式
engine, results = execute_engine(job_datetime, placeholders, loaded_data, FIXED_JSON5_ENGINE)

# 使用标准JSON格式
engine, results = execute_engine(job_datetime, placeholders, loaded_data, STANDARD_JSON_ENGINE)

# 方法2: 使用函数获取
from fixed_engine_string import get_engine_string

# 获取JSON5格式
json5_engine = get_engine_string(use_json5=True)
engine, results = execute_engine(job_datetime, placeholders, loaded_data, json5_engine)

# 获取标准JSON格式
json_engine = get_engine_string(use_json5=False)
engine, results = execute_engine(job_datetime, placeholders, loaded_data, json_engine)
    """)

if __name__ == "__main__":
    main()
    show_usage_example() 