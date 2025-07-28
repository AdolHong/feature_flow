#!/usr/bin/env python3
"""
从JSON5文件加载规则引擎配置的示例
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.engine import RuleEngine

def load_engine_from_json5():
    """从JSON5文件加载引擎配置"""
    print("=== 从JSON5文件加载引擎配置 ===")
    
    try:
        # 从JSON5文件加载引擎
        engine = RuleEngine.load_from_file("example_engine_config.json5")
        
        print(f"成功加载引擎: {engine.name}")
        print(f"节点数量: {len(engine.get_all_nodes())}")
        print()
        
        # 显示引擎信息
        print("引擎详细信息:")
        engine_info = engine.get_engine_info()
        for key, value in engine_info.items():
            print(f"  {key}: {value}")
        print()
        
        # 显示节点信息
        print("节点信息:")
        for node_name in engine.get_all_nodes():
            if node_name != "start_node":
                node_info = engine.get_node_info(node_name)
                print(f"  {node_name} ({node_info['type']}):")
                print(f"    依赖: {node_info['dependencies']}")
                print(f"    跟踪变量: {node_info['tracked_variables']}")
                if node_info.get('logic_code'):
                    print(f"    逻辑代码: {node_info['logic_code'][:50]}...")
                if node_info.get('condition'):
                    print(f"    条件: {node_info['condition'][:50]}...")
                print()
        
        # 显示执行顺序
        print("执行顺序:")
        execution_order = engine.get_execution_order()
        print(f"  {' -> '.join(execution_order)}")
        print()
        
        # 显示可视化流程
        print("流程可视化:")
        print(engine.visualize_flow())
        print()
        
        # 测试导出功能
        print("测试导出功能:")
        exported_json5 = engine.export_to_json()
        print("导出的JSON5片段:")
        print(exported_json5[:500] + "...")
        print()
        
        return engine
        
    except Exception as e:
        print(f"加载引擎配置失败: {e}")
        return None

def test_engine_execution(engine):
    """测试引擎执行"""
    if not engine:
        print("引擎未加载，跳过执行测试")
        return
    
    print("=== 测试引擎执行 ===")
    
    try:
        # 设置执行参数
        job_date = "2024-01-15"
        placeholders = {
            "data_source": "mock_database"
        }
        
        print(f"执行参数:")
        print(f"  作业日期: {job_date}")
        print(f"  占位符: {placeholders}")
        print()
        
        # 执行引擎
        results = engine.execute(
            job_date=job_date,
            placeholders=placeholders
        )
        
        # 显示执行结果
        print("执行结果:")
        for node_name, result in results.items():
            if node_name == "start_node":
                continue
            print(f"  {node_name}:")
            print(f"    成功: {result.success}")
            if not result.success:
                print(f"    错误: {result.error}")
            print()
        
        # 显示执行摘要
        summary = engine.get_execution_summary()
        print("执行摘要:")
        print(f"  总节点数: {summary['total_nodes']}")
        print(f"  成功节点数: {summary['successful_nodes']}")
        print(f"  失败节点数: {summary['failed_nodes']}")
        print(f"  阻断节点数: {summary['blocked_nodes']}")
        print(f"  成功率: {summary['success_rate']:.2%}")
        
        # 显示最终输出
        final_outputs = engine.get_final_outputs()
        if final_outputs:
            print("\n最终输出:")
            for node_name, output in final_outputs.items():
                print(f"  {node_name}: {type(output)}")
        
    except Exception as e:
        print(f"引擎执行失败: {e}")

def main():
    """主函数"""
    print("JSON5引擎配置加载示例")
    print("=" * 50)
    
    # 加载引擎
    engine = load_engine_from_json5()
    
    # 测试执行
    test_engine_execution(engine)
    
    print("\n示例完成!")

if __name__ == "__main__":
    main() 