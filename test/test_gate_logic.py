#!/usr/bin/env python3
"""
测试Gate节点逻辑的单元测试
验证：只要有一个gate通过，后续节点就应该执行
"""

import unittest
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.engine import RuleEngine
from core.nodes import LogicNode, GateNode


class TestGateLogic(unittest.TestCase):
    """测试Gate节点逻辑"""
    
    def setUp(self):
        """测试前的准备工作"""
        self.engine = RuleEngine("test_gate_logic")
    
    def test_single_gate_pass(self):
        """测试单个gate通过的情况"""
        # 创建逻辑节点：设置var=1
        preprocess_node = LogicNode("preprocess")
        preprocess_node.set_logic("var = 1")
        preprocess_node.set_tracked_variables(["var"])
        
        # 创建gate节点：var < 2 (应该通过)
        gate_node = GateNode("gate_lt_2")
        gate_node.set_condition("var < 2")
        
        # 创建输出节点
        output_node = LogicNode("output")
        output_node.add_expected_input_schema("var", "int")
        output_node.set_logic("output = var")
        output_node.set_tracked_variables(["output"])
        
        # 设置依赖关系
        self.engine.add_dependency(None, preprocess_node)
        self.engine.add_dependency(preprocess_node, gate_node)
        self.engine.add_dependency(gate_node, output_node)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        self.assertTrue(results["preprocess"].success)
        self.assertTrue(results["gate_lt_2"].success)
        self.assertTrue(results["output"].success)
        
        # 验证gate节点通过
        gate_result = results["gate_lt_2"]
        self.assertTrue(gate_result.data["should_continue"])
        
        # 验证输出节点执行了
        output_result = results["output"]
        self.assertEqual(output_result.data["output"], 1)
    
    def test_single_gate_block(self):
        """测试单个gate阻止的情况"""
        # 创建逻辑节点：设置var=3
        preprocess_node = LogicNode("preprocess")
        preprocess_node.set_logic("var = 3")
        preprocess_node.set_tracked_variables(["var"])
        
        # 创建gate节点：var < 2 (应该阻止)
        gate_node = GateNode("gate_lt_2")
        gate_node.set_condition("var < 2")
        
        # 创建输出节点
        output_node = LogicNode("output")
        output_node.add_expected_input_schema("var", "int")
        output_node.set_logic("output = var")
        output_node.set_tracked_variables(["output"])
        
        # 设置依赖关系
        self.engine.add_dependency(None, preprocess_node)
        self.engine.add_dependency(preprocess_node, gate_node)
        self.engine.add_dependency(gate_node, output_node)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        self.assertTrue(results["preprocess"].success)
        self.assertTrue(results["gate_lt_2"].success)
        self.assertFalse(results["output"].success)  # 应该被阻止
        
        # 验证gate节点阻止
        gate_result = results["gate_lt_2"]
        self.assertFalse(gate_result.data["should_continue"])
        
        # 验证输出节点被阻止
        output_result = results["output"]
        self.assertEqual(output_result.status, "blocked")
    
    def test_two_gates_one_pass(self):
        """测试两个gate，一个通过一个阻止的情况"""
        # 创建逻辑节点：设置var=1
        preprocess_node = LogicNode("preprocess")
        preprocess_node.set_logic("var = 1")
        preprocess_node.set_tracked_variables(["var"])
        
        # 创建两个gate节点
        gate_lt_2_node = GateNode("gate_lt_2")
        gate_lt_2_node.set_condition("var < 2")  # 应该通过
        
        gate_gte_2_node = GateNode("gate_gte_2")
        gate_gte_2_node.set_condition("var >= 2")  # 应该阻止
        
        # 创建输出节点
        output_node = LogicNode("output")
        output_node.add_expected_input_schema("var", "int")
        output_node.set_logic("output = var")
        output_node.set_tracked_variables(["output"])
        
        # 设置依赖关系：两个gate都连接到output
        self.engine.add_dependency(None, preprocess_node)
        self.engine.add_dependency(preprocess_node, gate_lt_2_node)
        self.engine.add_dependency(preprocess_node, gate_gte_2_node)
        self.engine.add_dependency(gate_lt_2_node, output_node)
        self.engine.add_dependency(gate_gte_2_node, output_node)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        self.assertTrue(results["preprocess"].success)
        self.assertTrue(results["gate_lt_2"].success)
        self.assertTrue(results["gate_gte_2"].success)
        self.assertTrue(results["output"].success)  # 应该执行，因为有一个gate通过
        
        # 验证gate节点状态
        gate_lt_2_result = results["gate_lt_2"]
        gate_gte_2_result = results["gate_gte_2"]
        self.assertTrue(gate_lt_2_result.data["should_continue"])  # 通过
        self.assertFalse(gate_gte_2_result.data["should_continue"])  # 阻止
        
        # 验证输出节点执行了
        output_result = results["output"]
        self.assertEqual(output_result.data["output"], 1)
    
    def test_two_gates_both_block(self):
        """测试两个gate都阻止的情况"""
        # 创建逻辑节点：设置var=3
        preprocess_node = LogicNode("preprocess")
        preprocess_node.set_logic("var = 3")
        preprocess_node.set_tracked_variables(["var"])
        
        # 创建两个gate节点
        gate_lt_2_node = GateNode("gate_lt_2")
        gate_lt_2_node.set_condition("var < 2")  # 应该阻止
        
        gate_eq_5_node = GateNode("gate_eq_5")
        gate_eq_5_node.set_condition("var == 5")  # 应该阻止
        
        # 创建输出节点
        output_node = LogicNode("output")
        output_node.add_expected_input_schema("var", "int")
        output_node.set_logic("output = var")
        output_node.set_tracked_variables(["output"])
        
        # 设置依赖关系：两个gate都连接到output
        self.engine.add_dependency(None, preprocess_node)
        self.engine.add_dependency(preprocess_node, gate_lt_2_node)
        self.engine.add_dependency(preprocess_node, gate_eq_5_node)
        self.engine.add_dependency(gate_lt_2_node, output_node)
        self.engine.add_dependency(gate_eq_5_node, output_node)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        self.assertTrue(results["preprocess"].success)
        self.assertTrue(results["gate_lt_2"].success)
        self.assertTrue(results["gate_eq_5"].success)
        self.assertFalse(results["output"].success)  # 应该被阻止，因为所有gate都阻止
        
        # 验证gate节点状态
        gate_lt_2_result = results["gate_lt_2"]
        gate_eq_5_result = results["gate_eq_5"]
        self.assertFalse(gate_lt_2_result.data["should_continue"])  # 阻止
        self.assertFalse(gate_eq_5_result.data["should_continue"])  # 阻止
        
        # 验证输出节点被阻止
        output_result = results["output"]
        self.assertEqual(output_result.status, "blocked")
    
    def test_mixed_dependencies(self):
        """测试混合依赖：gate节点和其他类型节点"""
        # 创建逻辑节点：设置var=1
        preprocess_node = LogicNode("preprocess")
        preprocess_node.set_logic("var = 1")
        preprocess_node.set_tracked_variables(["var"])
        
        # 创建gate节点
        gate_node = GateNode("gate_lt_2")
        gate_node.set_condition("var < 2")
        
        # 创建另一个逻辑节点
        logic_node = LogicNode("logic_node")
        logic_node.set_logic("processed_var = var * 2")
        logic_node.set_tracked_variables(["processed_var"])
        
        # 创建输出节点
        output_node = LogicNode("output")
        output_node.add_expected_input_schema("var", "int")
        output_node.add_expected_input_schema("processed_var", "int")
        output_node.set_logic("output = var + processed_var")
        output_node.set_tracked_variables(["output"])
        
        # 设置依赖关系
        self.engine.add_dependency(None, preprocess_node)
        self.engine.add_dependency(preprocess_node, gate_node)
        self.engine.add_dependency(preprocess_node, logic_node)
        self.engine.add_dependency(gate_node, output_node)
        self.engine.add_dependency(logic_node, output_node)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        self.assertTrue(results["preprocess"].success)
        self.assertTrue(results["gate_lt_2"].success)
        self.assertTrue(results["logic_node"].success)
        self.assertTrue(results["output"].success)  # 应该执行，因为gate通过且logic_node成功
        
        # 验证输出
        output_result = results["output"]
        self.assertEqual(output_result.data["output"], 3)  # 1 + 2 = 3


if __name__ == "__main__":
    unittest.main() 