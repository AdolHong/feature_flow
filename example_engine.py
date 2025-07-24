#!/usr/bin/env python3
"""
示例程序：从Redis加载数据并使用规则引擎处理
"""

from core.engine import RuleEngine
from core.nodes import LogicNode


def create_rule_engine():
    """创建规则引擎"""
    engine = RuleEngine("demo")
    
    # 创建逻辑节点1：数据预处理
    logic1_node = LogicNode("logic1")
    logic1_node.set_logic("""
var1 = 1
""")
    logic1_node.set_tracked_variables(["var1"])

    logic2_node = LogicNode("logic2")
    logic2_node.set_logic("""
var2 = 2
""")
    logic2_node.set_tracked_variables(["var2"])
    
    
    output_node = LogicNode("output")
    output_node.add_expected_input_schema("var1", "int")
    output_node.add_expected_input_schema("var2", "int")
    output_node.set_logic("""
output = var1 + var2
""")
    output_node.set_tracked_variables(["output"])

    # 校验输出
    validate_output_node = LogicNode("validate_output")
    validate_output_node.add_expected_input_schema("output", "int")
    validate_output_node.set_logic("""
output = output
""")
    validate_output_node.set_tracked_variables(["output"])

    # 设置依赖关系
    engine.add_dependency(None, logic1_node)
    engine.add_dependency(None, logic2_node)
    engine.add_dependency(logic1_node, output_node)
    engine.add_dependency(logic2_node, output_node)
    engine.add_dependency(output_node, validate_output_node)
    return engine


def main():
    engine = create_rule_engine()
    results = engine.execute()
    print(results)

if __name__ == "__main__":
    main() 