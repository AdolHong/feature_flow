#!/usr/bin/env python3
"""
测试新的导入支持功能
"""

from core.engine import RuleEngine
from core.nodes import LogicNode, CollectionNode

def test_logic_node_import():
    """测试LogicNode中的动态导入功能"""
    print("=== 测试LogicNode中的动态导入功能 ===")
    
    # 创建引擎
    engine = RuleEngine("import_test")
    
    # 创建LogicNode，测试各种导入
    logic_node = LogicNode("test_imports")
    logic_node.set_logic("""
# 测试各种导入操作
from datetime import datetime, timedelta
import json
import os
import sys

# 使用导入的模块
current_time = datetime.now()
time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
json_data = json.dumps({"time": time_str, "test": True})
path_exists = os.path.exists("/tmp")
python_version = sys.version

print(f"当前时间: {time_str}")
print(f"JSON数据: {json_data}")
print(f"路径存在: {path_exists}")
print(f"Python版本: {python_version}")

# 转换为可序列化的格式
current_time_str = current_time.isoformat()
""")
    
    logic_node.set_tracked_variables([
        "current_time_str", "time_str", "json_data", 
        "path_exists", "python_version"
    ])
    
    # 添加依赖关系
    engine.add_dependency(None, logic_node)
    
    # 执行引擎
    results = engine.execute()
    
    # 验证结果
    if results["test_imports"].success:
        print("✅ LogicNode导入测试成功!")
        print(f"输出数据: {results['test_imports'].data}")
        print(f"文本输出: {results['test_imports'].text_output}")
    else:
        print(f"❌ LogicNode导入测试失败: {results['test_imports'].error}")
    
    return results["test_imports"].success

def test_collection_node_import():
    """测试CollectionNode中的动态导入功能"""
    print("\n=== 测试CollectionNode中的动态导入功能 ===")
    
    # 创建引擎
    engine = RuleEngine("collection_import_test")
    
    # 创建第一个LogicNode提供数据
    logic1 = LogicNode("data_provider")
    logic1.set_logic("""
import random
data = [random.randint(1, 100) for _ in range(5)]
print(f"生成的数据: {data}")
""")
    logic1.set_tracked_variables(["data"])
    
    # 创建CollectionNode，测试导入
    collection_node = CollectionNode("test_collection_imports")
    collection_node.set_logic("""
# 测试在CollectionNode中的导入
import numpy as np
import pandas as pd
from datetime import datetime

# 处理收集的数据
all_data = []
for node_name, node_data in collection.items():
    if 'data' in node_data:
        all_data.extend(node_data['data'])

# 使用numpy和pandas处理数据
np_array = np.array(all_data)
mean_value = float(np.mean(np_array))
df = pd.DataFrame({'values': all_data})
summary_dict = df.describe().to_dict()

current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"所有数据: {all_data}")
print(f"平均值: {mean_value}")
print(f"数据摘要: {summary_dict}")
print(f"处理时间: {current_time}")
""")
    
    collection_node.set_tracked_variables([
        "all_data", "mean_value", "summary_dict", "current_time"
    ])
    
    # 添加依赖关系
    engine.add_dependency(None, logic1)
    engine.add_dependency(logic1, collection_node)
    
    # 执行引擎
    results = engine.execute()
    
    # 验证结果
    if results["test_collection_imports"].success:
        print("✅ CollectionNode导入测试成功!")
        print(f"输出数据: {results['test_collection_imports'].data}")
        print(f"文本输出: {results['test_collection_imports'].text_output}")
    else:
        print(f"❌ CollectionNode导入测试失败: {results['test_collection_imports'].error}")
    
    return results["test_collection_imports"].success

def test_custom_module_import():
    """测试自定义模块的导入"""
    print("\n=== 测试自定义模块的导入 ===")
    
    # 创建引擎
    engine = RuleEngine("custom_import_test")
    
    # 创建LogicNode，测试导入自定义模块
    logic_node = LogicNode("custom_imports")
    logic_node.set_logic("""
# 测试导入自定义模块
try:
    from core.nodes import NodeResult
    from core.engine import RuleEngine
    
    # 创建一些测试对象
    result_dict = {"test": "custom_module", "success": True}
    engine_name = RuleEngine.__name__
    
    print(f"成功导入自定义模块!")
    print(f"结果字典: {result_dict}")
    print(f"引擎类名: {engine_name}")
    
except ImportError as e:
    print(f"导入失败: {e}")
    result_dict = None
    engine_name = None
""")
    
    logic_node.set_tracked_variables(["result_dict", "engine_name"])
    
    # 添加依赖关系
    engine.add_dependency(None, logic_node)
    
    # 执行引擎
    results = engine.execute()
    
    # 验证结果
    if results["custom_imports"].success:
        print("✅ 自定义模块导入测试成功!")
        print(f"输出数据: {results['custom_imports'].data}")
        print(f"文本输出: {results['custom_imports'].text_output}")
    else:
        print(f"❌ 自定义模块导入测试失败: {results['custom_imports'].error}")
    
    return results["custom_imports"].success

if __name__ == "__main__":
    print("开始测试新的导入支持功能...\n")
    
    # 运行所有测试
    test1_success = test_logic_node_import()
    test2_success = test_collection_node_import()
    test3_success = test_custom_module_import()
    
    print("\n" + "="*50)
    print("测试结果汇总:")
    print(f"LogicNode导入测试: {'✅ 通过' if test1_success else '❌ 失败'}")
    print(f"CollectionNode导入测试: {'✅ 通过' if test2_success else '❌ 失败'}")
    print(f"自定义模块导入测试: {'✅ 通过' if test3_success else '❌ 失败'}")
    
    if all([test1_success, test2_success, test3_success]):
        print("\n🎉 所有测试都通过了! 导入支持功能正常工作。")
    else:
        print("\n⚠️  部分测试失败，请检查错误信息。")
