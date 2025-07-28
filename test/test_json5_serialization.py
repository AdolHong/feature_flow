#!/usr/bin/env python3
"""
测试JSON5序列化和反序列化功能
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.engine import RuleEngine
from core.nodes import LogicNode, GateNode, CollectionNode

def test_json5_serialization():
    """测试JSON5序列化和反序列化"""
    print("=== 测试JSON5序列化和反序列化 ===")
    
    # 创建规则引擎
    engine = RuleEngine("test_engine")
    
    # 添加一些节点
    logic_node = LogicNode("logic_1")
    logic_node.set_logic("result = input_data * 2")
    logic_node.set_tracked_variables(["result"])
    engine.add_node(logic_node)
    
    gate_node = GateNode("gate_1")
    gate_node.set_condition("input_data > 10")
    gate_node.set_tracked_variables(["should_continue"])
    engine.add_node(gate_node)
    
    collection_node = CollectionNode("collection_1")
    collection_node.set_logic("collected = [input_data, result]")
    collection_node.set_tracked_variables(["collected"])
    engine.add_node(collection_node)
    
    # 添加依赖关系
    engine.add_dependency(None, logic_node)  # start -> logic_1
    engine.add_dependency(logic_node, gate_node)  # logic_1 -> gate_1
    engine.add_dependency(gate_node, collection_node)  # gate_1 -> collection_1
    
    # 设置期望输入schema
    engine.set_node_expected_input_schema("logic_1", {"input_data": "number"})
    engine.set_node_expected_input_schema("gate_1", {"input_data": "number"})
    engine.set_node_expected_input_schema("collection_1", {"input_data": "number", "result": "number"})
    
    print("原始引擎配置:")
    print(engine.visualize_flow())
    print()
    
    # 导出为JSON5
    json5_str = engine.export_to_json()
    print("导出的JSON5:")
    print(json5_str)
    print()
    
    # 从JSON5重新创建引擎
    new_engine = RuleEngine.import_from_json(json5_str)
    print("重新创建的引擎配置:")
    print(new_engine.visualize_flow())
    print()
    
    # 验证两个引擎是否相同
    print("验证结果:")
    print(f"节点数量相同: {len(engine.get_all_nodes()) == len(new_engine.get_all_nodes())}")
    print(f"依赖关系相同: {engine.data_flow.dependencies == new_engine.data_flow.dependencies}")
    
    # 测试文件保存和加载
    test_file = "test_engine.json5"
    try:
        engine.save_to_file(test_file)
        print(f"引擎配置已保存到: {test_file}")
        
        loaded_engine = RuleEngine.load_from_file(test_file)
        print("从文件加载的引擎配置:")
        print(loaded_engine.visualize_flow())
        
        # 清理测试文件
        os.remove(test_file)
        print(f"测试文件已删除: {test_file}")
        
    except Exception as e:
        print(f"文件操作错误: {e}")
    
    print("\n=== 测试完成 ===")

def test_json5_features():
    """测试JSON5的特殊功能"""
    print("\n=== 测试JSON5特殊功能 ===")
    
    # 创建包含JSON5特殊语法的配置
    config_with_comments = '''
    {
        // 这是一个JSON5配置文件
        name: "test_engine_with_comments",  // 支持无引号键名
        nodes: {
            logic_1: {
                type: "LogicNode",
                name: "logic_1",
                logic_code: "result = input_data * 2",  // 支持注释
                tracked_variables: ["result"],
                expected_input_schema: {
                    input_data: "number"  // 支持无引号键名
                }
            }
        },
        dependencies: {
            logic_1: ["start_node"]  // 支持尾随逗号
        }
    }
    '''
    
    try:
        # 测试从JSON5字符串创建引擎
        engine = RuleEngine.import_from_json(config_with_comments)
        print("成功从JSON5字符串创建引擎:")
        print(engine.visualize_flow())
        
        # 测试导出
        exported = engine.export_to_json()
        print("\n导出的JSON5:")
        print(exported)
        
    except Exception as e:
        print(f"JSON5解析错误: {e}")
    
    print("\n=== JSON5特殊功能测试完成 ===")

if __name__ == "__main__":
    test_json5_serialization()
    test_json5_features() 