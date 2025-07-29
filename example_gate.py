#!/usr/bin/env python3
"""
示例程序：从Redis加载数据并使用规则引擎处理
"""

from core.engine import RuleEngine
from core.nodes import LogicNode, GateNode


def create_rule_engine():
    """创建规则引擎"""
    engine = RuleEngine("demo")
    
    # 创建逻辑节点1：数据预处理
    preprocess_node = LogicNode("preprocess")
    preprocess_node.set_logic("""
var = 1
""")
    preprocess_node.set_tracked_variables(["var"])

    gate_lt_2_node = GateNode("gate_lt_2")
    gate_lt_2_node.set_condition("var < 2")
    gate_gt_2_node = GateNode("gate_gte_2")
    gate_gt_2_node.set_condition("var >= 2")
    output_node = LogicNode("output")
    output_node.add_expected_input_schema("var", "int")
    output_node.set_logic("""
output = var
""")
    output_node.set_tracked_variables(["output"])

    # 设置依赖关系
    engine.add_dependency(None, preprocess_node)
    engine.add_dependency(preprocess_node, gate_lt_2_node)
    engine.add_dependency(preprocess_node, gate_gt_2_node)
    engine.add_dependency(gate_lt_2_node, output_node)
    engine.add_dependency(gate_gt_2_node, output_node)
    return engine


def main():
    engine = create_rule_engine()
    results = engine.execute()
    print(results)

if __name__ == "__main__":
    main() 