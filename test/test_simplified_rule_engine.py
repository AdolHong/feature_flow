"""
简化版本规则引擎测试文件
测试start、logic、gate、collection四种节点的功能
"""

import unittest
import sys
import os
import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.engine import RuleEngine
from core.nodes import LogicNode, GateNode, CollectionNode


class TestSimplifiedRuleEngine(unittest.TestCase):
    """测试简化版本规则引擎"""
    
    def setUp(self):
        """测试前准备"""
        self.engine = RuleEngine("test_engine")
    
    def test_basic_flow(self):
        """测试基本流程"""
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("result = 42")
        logic1.set_tracked_variables(["result"])
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("final_result = result * 2")
        logic2.set_tracked_variables(["final_result"])
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(logic1, logic2)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        assert results["logic2"].success
        assert results["logic1"].data["result"] == 42
        assert results["logic2"].data["final_result"] == 84
        
        # 验证flow_context
        flow_context1 = self.engine.get_node_flow_context("logic1")
        flow_context2 = self.engine.get_node_flow_context("logic2")
        
        assert "result" in flow_context1
        assert "final_result" in flow_context2
        assert "result" in flow_context2  # 继承的变量

    def test_gate_node(self):
        """测试门控节点"""
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("value = 10")
        logic1.set_tracked_variables(["value"])
        
        gate = GateNode("gate1")
        gate.set_condition("value > 5")
        gate.set_tracked_variables(["should_continue"])
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("result = value * 2")
        logic2.set_tracked_variables(["result"])
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(logic1, gate)
        self.engine.add_dependency(gate, logic2)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        assert results["gate1"].success
        assert results["logic2"].success
        assert results["gate1"].data["should_continue"] == True
        assert results["logic2"].data["result"] == 20

    def test_gate_node_block(self):
        """测试门控节点阻止执行"""
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("value = 3")
        logic1.set_tracked_variables(["value"])
        
        gate = GateNode("gate1")
        gate.set_condition("value > 5")
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("result = value * 2")
        logic2.set_tracked_variables(["result"])
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(logic1, gate)
        self.engine.add_dependency(gate, logic2)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        assert results["gate1"].success
        assert results["gate1"].data["should_continue"] == False
        # logic2应该被阻止执行
        assert not results["logic2"].success
        assert "blocked" in results["logic2"].status

    def test_collection_node(self):
        """测试收集节点"""
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("result = 10")
        logic1.set_tracked_variables(["result"])
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("result = 20")
        logic2.set_tracked_variables(["result"])
        
        collection = CollectionNode("collection1")
        collection.add_expected_input_schema("result", "int")
        collection.set_logic("total = sum(data['result'] for data in collection.values())")
        collection.set_tracked_variables(["total"])
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(None, logic2)
        self.engine.add_dependency(logic1, collection)
        self.engine.add_dependency(logic2, collection)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        assert results["logic2"].success
        assert results["collection1"].success
        assert results["collection1"].data["total"] == 30

    def test_collection_node_skip_invalid(self):
        """测试收集节点跳过无效数据"""
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("result = 'not an int'")
        logic1.set_tracked_variables(["result"])
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("result = 42")
        logic2.set_tracked_variables(["result"])
        
        collection = CollectionNode("collection1")
        collection.set_logic("valid_count = len([data for data in collection.values() if isinstance(data['result'], int)])")
        collection.set_tracked_variables(["valid_count"])
        collection.set_expected_input_schema({"result": "int"})
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(None, logic2)
        self.engine.add_dependency(logic1, collection)
        self.engine.add_dependency(logic2, collection)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        assert results["logic2"].success
        assert results["collection1"].success
        # 只有logic2的数据符合schema，所以valid_count应该是1
        assert results["collection1"].data["valid_count"] == 1

    def test_expected_input_schema(self):
        """测试期望输入schema"""
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("value = 42")
        logic1.set_tracked_variables(["value"])
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("result = value * 2")
        logic2.set_tracked_variables(["result"])
        logic2.set_expected_input_schema({"value": "int"})
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(logic1, logic2)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        assert results["logic2"].success
        assert results["logic2"].data["result"] == 84

    def test_expected_input_schema_validation_fail(self):
        """测试期望输入schema校验失败"""
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("value = 'not an int'")
        logic1.set_tracked_variables(["value"])
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("result = value * 2")
        logic2.set_tracked_variables(["result"])
        logic2.set_expected_input_schema({"value": "int"})
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(logic1, logic2)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        # logic2应该因为schema校验失败而失败
        assert not results["logic2"].success
        assert "validation failed" in results["logic2"].error

    def test_global_schema(self):
        """测试全局schema"""
        # 设置全局schema
        self.engine.set_global_schema({
            "trend": "double",
            "seasonal": "double",
            "residual": "double"
        })
        
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("trend = 0.5\nseasonal = 0.3\nresidual = 0.2")
        logic1.set_tracked_variables(["trend", "seasonal", "residual"])
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("total = trend + seasonal + residual")
        logic2.set_tracked_variables(["total"])
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(logic1, logic2)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        assert results["logic2"].success
        assert abs(results["logic2"].data["total"] - 1.0) < 1e-6

    def test_flow_context_inheritance(self):
        """测试flow_context继承"""
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("value1 = 10\nvalue2 = 20")
        logic1.set_tracked_variables(["value1", "value2"])
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("value3 = value1 + value2")
        logic2.set_tracked_variables(["value3"])
        
        logic3 = LogicNode("logic3")
        logic3.set_logic("final = value1 + value2 + value3")
        logic3.set_tracked_variables(["final"])
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(logic1, logic2)
        self.engine.add_dependency(logic2, logic3)
        
        # 执行引擎
        results = self.engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        assert results["logic2"].success
        assert results["logic3"].success
        assert results["logic3"].data["final"] == 60  # 10 + 20 + 30
        
        # 验证flow_context继承
        flow_context3 = self.engine.get_node_flow_context("logic3")
        assert "value1" in flow_context3
        assert "value2" in flow_context3
        assert "value3" in flow_context3
        assert "final" in flow_context3

    def test_dynamic_parameters(self):
        """测试动态参数"""
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("date_str = '${yyyy-MM-dd}'\nvalue = 42")
        logic1.set_tracked_variables(["date_str", "value"])
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        
        # 执行引擎，提供动态参数
        job_date = "2024-01-15 10:30:00"
        results = self.engine.execute(job_date=job_date)
        
        # 验证结果
        assert results["logic1"].success
        assert results["logic1"].data["date_str"] == "2024-01-15"
        assert results["logic1"].data["value"] == 42

    def test_export_import(self):
        """测试导出导入功能"""
        # 设置全局schema
        self.engine.set_global_schema({"trend": "double"})
        
        # 创建节点
        logic1 = LogicNode("logic1")
        logic1.set_logic("trend = 0.5")
        logic1.set_tracked_variables(["trend"])
        
        logic2 = LogicNode("logic2")
        logic2.set_logic("result = trend * 2")
        logic2.set_tracked_variables(["result"])
        logic2.set_expected_input_schema({"trend": "double"})
        
        # 添加依赖关系，自动添加节点
        self.engine.add_dependency(None, logic1)
        self.engine.add_dependency(logic1, logic2)
        
        # 导出配置
        json_config = self.engine.export_to_json()
        
        # 创建新引擎并导入配置
        new_engine = RuleEngine("test_engine_new")
        new_engine.import_from_json(json_config)
        
        # 验证配置被正确导入
        assert new_engine.get_global_schema() == {"trend": "double"}
        
        # 执行新引擎
        results = new_engine.execute()
        
        # 验证结果
        assert results["logic1"].success
        assert results["logic2"].success
        assert results["logic2"].data["result"] == 1.0


if __name__ == '__main__':
    unittest.main() 