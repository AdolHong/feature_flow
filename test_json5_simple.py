#!/usr/bin/env python3
"""
简化的JSON5序列化和反序列化测试
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json5

def test_json5_basic():
    """测试JSON5基本功能"""
    print("=== 测试JSON5基本功能 ===")
    
    # 测试JSON5的特殊语法
    json5_str = '''
    {
        // 这是一个JSON5配置文件
        name: "test_engine",  // 支持无引号键名
        version: 1.0,
        enabled: true,
        settings: {
            timeout: 30,  // 支持注释
            retries: 3,
            debug: false,
        },
        nodes: [
            "node1",
            "node2",  // 支持尾随逗号
        ],
        metadata: {
            created: "2024-01-01",
            author: "test_user",
        }
    }
    '''
    
    try:
        # 解析JSON5
        data = json5.loads(json5_str)
        print("成功解析JSON5:")
        print(f"名称: {data['name']}")
        print(f"版本: {data['version']}")
        print(f"启用状态: {data['enabled']}")
        print(f"节点数量: {len(data['nodes'])}")
        print(f"设置: {data['settings']}")
        print()
        
        # 重新序列化为JSON5
        serialized = json5.dumps(data, indent=2, ensure_ascii=False)
        print("重新序列化的JSON5:")
        print(serialized)
        print()
        
        # 验证数据一致性
        reparsed = json5.loads(serialized)
        print("验证数据一致性:")
        print(f"数据相同: {data == reparsed}")
        
    except Exception as e:
        print(f"JSON5解析错误: {e}")
    
    print("\n=== JSON5基本功能测试完成 ===")

def test_json5_engine_compatibility():
    """测试JSON5与引擎配置的兼容性"""
    print("\n=== 测试JSON5与引擎配置兼容性 ===")
    
    # 模拟引擎配置
    engine_config = {
        'name': 'test_engine',
        'nodes': {
            'logic_1': {
                'type': 'LogicNode',
                'name': 'logic_1',
                'logic_code': 'result = input_data * 2',
                'tracked_variables': ['result'],
                'expected_input_schema': {
                    'input_data': 'number'
                }
            },
            'gate_1': {
                'type': 'GateNode',
                'name': 'gate_1',
                'condition': 'input_data > 10',
                'tracked_variables': ['should_continue'],
                'expected_input_schema': {
                    'input_data': 'number'
                }
            }
        },
        'dependencies': {
            'logic_1': ['start_node'],
            'gate_1': ['logic_1']
        }
    }
    
    try:
        # 序列化为JSON5
        json5_str = json5.dumps(engine_config, indent=2, ensure_ascii=False)
        print("引擎配置序列化为JSON5:")
        print(json5_str)
        print()
        
        # 从JSON5重新解析
        reparsed_config = json5.loads(json5_str)
        print("从JSON5重新解析的配置:")
        print(f"引擎名称: {reparsed_config['name']}")
        print(f"节点数量: {len(reparsed_config['nodes'])}")
        print(f"依赖关系数量: {len(reparsed_config['dependencies'])}")
        print()
        
        # 验证数据完整性
        print("验证数据完整性:")
        print(f"配置相同: {engine_config == reparsed_config}")
        print(f"节点类型正确: {reparsed_config['nodes']['logic_1']['type'] == 'LogicNode'}")
        print(f"门控节点类型正确: {reparsed_config['nodes']['gate_1']['type'] == 'GateNode'}")
        
    except Exception as e:
        print(f"引擎配置JSON5测试错误: {e}")
    
    print("\n=== JSON5引擎配置兼容性测试完成 ===")

def test_json5_file_operations():
    """测试JSON5文件操作"""
    print("\n=== 测试JSON5文件操作 ===")
    
    test_data = {
        'name': 'test_engine',
        'version': 1.0,
        'nodes': {
            'logic_1': {
                'type': 'LogicNode',
                'logic_code': 'result = input_data * 2'
            }
        }
    }
    
    test_file = "test_config.json5"
    
    try:
        # 保存到文件
        with open(test_file, 'w', encoding='utf-8') as f:
            json5.dump(test_data, f, indent=2, ensure_ascii=False)
        print(f"配置已保存到: {test_file}")
        
        # 从文件读取
        with open(test_file, 'r', encoding='utf-8') as f:
            loaded_data = json5.load(f)
        print("从文件加载的配置:")
        print(f"引擎名称: {loaded_data['name']}")
        print(f"版本: {loaded_data['version']}")
        print(f"节点数量: {len(loaded_data['nodes'])}")
        
        # 清理测试文件
        os.remove(test_file)
        print(f"测试文件已删除: {test_file}")
        
    except Exception as e:
        print(f"文件操作错误: {e}")
    
    print("\n=== JSON5文件操作测试完成 ===")

if __name__ == "__main__":
    test_json5_basic()
    test_json5_engine_compatibility()
    test_json5_file_operations() 