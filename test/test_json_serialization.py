#!/usr/bin/env python3
"""
测试 JSON 序列化和反序列化功能
"""

import json
import pandas as pd
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.json_helper import UniversalEncoder, universal_decoder

def test_json_serialization():
    """测试 JSON 序列化和反序列化"""
    print("🧪 测试 JSON 序列化和反序列化功能")
    
    # 1. 创建包含 DataFrame 的字典
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'city': ['北京', '上海', '深圳']
    })
    
    data = {"tmp": df}
    
    print("\n原始数据:")
    print(f"类型: {type(data)}")
    print(f"data['tmp'] 类型: {type(data['tmp'])}")
    print(data['tmp'])
    
    # 2. 序列化为 JSON 字符串
    string = json.dumps(data, cls=UniversalEncoder, ensure_ascii=False)
    print(f"\n序列化后的 JSON 字符串:")
    print(string)
    
    # 3. 从 JSON 字符串还原数据
    restored_data = json.loads(string, object_hook=universal_decoder)
    print(f"\n还原后的数据:")
    print(f"类型: {type(restored_data)}")
    print(f"restored_data['tmp'] 类型: {type(restored_data['tmp'])}")
    print(restored_data['tmp'])
    
    # 4. 验证数据是否一致
    is_equal = data['tmp'].equals(restored_data['tmp'])
    print(f"\n数据是否一致: {is_equal}")
    
    if is_equal:
        print("✅ JSON 序列化和反序列化测试通过！")
    else:
        print("❌ JSON 序列化和反序列化测试失败！")

if __name__ == "__main__":
    test_json_serialization() 