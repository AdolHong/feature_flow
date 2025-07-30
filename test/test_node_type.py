"""
测试节点类型功能
测试Node基类的node_type属性和相关功能
"""

import pytest
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core.nodes import (
    Node, NodeResult, StartNode, LogicNode, GateNode, CollectionNode
)
from core.engine import RuleEngine


class TestNodeType:
    """测试节点类型功能"""
    
    def test_node_base_class_has_node_type(self):
        """测试Node基类有node_type属性"""
        # 创建一个测试用的Node子类
        class TestNode(Node):
            def execute(self, context, job_date=None, placeholders=None):
                return NodeResult(success=True, data={}, node_type=self.node_type)
        
        node = TestNode("test_node")
        assert hasattr(node, 'node_type')
        assert node.node_type == "unknown"  # 默认应该是unknown
    
    def test_start_node_type(self):
        """测试StartNode的节点类型"""
        node = StartNode("test_start")
        assert node.node_type == "start"
        
        result = node.execute({})
        assert result.node_type == "start"
    
    def test_logic_node_type(self):
        """测试LogicNode的节点类型"""
        node = LogicNode("test_logic")
        assert node.node_type == "logic"
        
        # 设置逻辑代码
        node.set_logic("x = 1")
        node.add_tracked_variable("x")
        
        result = node.execute({})
        assert result.node_type == "logic"
    
    def test_gate_node_type(self):
        """测试GateNode的节点类型"""
        node = GateNode("test_gate")
        assert node.node_type == "gate"
        
        # 设置条件
        node.set_condition("True")
        
        result = node.execute({})
        assert result.node_type == "gate"
    
    def test_collection_node_type(self):
        """测试CollectionNode的节点类型"""
        node = CollectionNode("test_collection")
        assert node.node_type == "collection"
        
        result = node.execute({})
        assert result.node_type == "collection"
    
    def test_node_result_with_node_type(self):
        """测试NodeResult包含node_type"""
        result = NodeResult(
            success=True, 
            data={"test": "data"}, 
            node_type="logic"
        )
        assert result.node_type == "logic"
        assert "node_type=logic" in str(result)
    
    def test_node_result_repr_includes_node_type(self):
        """测试NodeResult的__repr__方法包含node_type"""
        result = NodeResult(
            success=True, 
            data={"test": "data"}, 
            text_output="test output",
            error=None,
            status="executed",
            node_type="gate"
        )
        repr_str = str(result)
        assert "node_type=gate" in repr_str
        assert "success=True" in repr_str
        assert "status=executed" in repr_str


class TestEngineNodeTypeSupport:
    """测试RuleEngine对node_type的支持"""
    
    def setup_method(self):
        """每个测试方法前的设置"""
        self.engine = RuleEngine("test_engine")
    
    def test_get_nodes_by_type(self):
        """测试根据节点类型获取节点"""
        # 添加不同类型的节点
        logic_node = LogicNode("logic_1")
        gate_node = GateNode("gate_1")
        collection_node = CollectionNode("collection_1")
        
        self.engine.add_node(logic_node)
        self.engine.add_node(gate_node)
        self.engine.add_node(collection_node)
        
        # 测试获取各类型节点
        logic_nodes = self.engine.get_nodes_by_type("logic")
        assert len(logic_nodes) == 1
        assert "logic_1" in logic_nodes
        
        gate_nodes = self.engine.get_nodes_by_type("gate")
        assert len(gate_nodes) == 1
        assert "gate_1" in gate_nodes
        
        collection_nodes = self.engine.get_nodes_by_type("collection")
        assert len(collection_nodes) == 1
        assert "collection_1" in collection_nodes
        
        # 测试获取不存在的类型
        unknown_nodes = self.engine.get_nodes_by_type("unknown")
        assert len(unknown_nodes) == 0
    
    def test_get_node_types_count(self):
        """测试获取节点类型数量统计"""
        # 添加多个不同类型的节点
        logic_node1 = LogicNode("logic_1")
        logic_node2 = LogicNode("logic_2")
        gate_node = GateNode("gate_1")
        collection_node = CollectionNode("collection_1")
        
        self.engine.add_node(logic_node1)
        self.engine.add_node(logic_node2)
        self.engine.add_node(gate_node)
        self.engine.add_node(collection_node)
        
        type_counts = self.engine.get_node_types_count()
        
        assert type_counts["logic"] == 2
        assert type_counts["gate"] == 1
        assert type_counts["collection"] == 1
        assert type_counts["start"] == 1  # 默认的start_node
    
    def test_get_node_info_includes_node_type(self):
        """测试get_node_info包含node_type信息"""
        logic_node = LogicNode("test_logic")
        self.engine.add_node(logic_node)
        
        node_info = self.engine.get_node_info("test_logic")
        
        assert "node_type" in node_info
        assert node_info["node_type"] == "logic"
        assert node_info["type"] == "LogicNode"
    
    def test_execution_results_with_node_type(self):
        """测试执行结果包含node_type"""
        # 创建并执行一个简单的逻辑节点
        logic_node = LogicNode("test_logic")
        logic_node.set_logic("x = 10")
        logic_node.add_tracked_variable("x")
        
        self.engine.add_node(logic_node)
        self.engine.add_dependency(None, logic_node)
        
        results = self.engine.execute()
        
        assert "test_logic" in results
        result = results["test_logic"]
        assert result.node_type == "logic"
        assert result.success
    
    def test_get_results_by_node_type(self):
        """测试根据节点类型获取执行结果"""
        # 创建多个不同类型的节点
        logic_node = LogicNode("logic_1")
        logic_node.set_logic("x = 1")
        logic_node.add_tracked_variable("x")
        
        gate_node = GateNode("gate_1")
        gate_node.set_condition("True")
        
        collection_node = CollectionNode("collection_1")
        
        # 添加到引擎
        self.engine.add_node(logic_node)
        self.engine.add_node(gate_node)
        self.engine.add_node(collection_node)
        
        # 添加依赖关系
        self.engine.add_dependency(None, logic_node)
        self.engine.add_dependency(logic_node, gate_node)
        self.engine.add_dependency(gate_node, collection_node)
        
        # 执行
        results = self.engine.execute()
        
        # 测试按类型获取结果
        logic_results = self.engine.get_results_by_node_type("logic")
        assert len(logic_results) == 1
        assert "logic_1" in logic_results
        assert logic_results["logic_1"].node_type == "logic"
        
        gate_results = self.engine.get_results_by_node_type("gate")
        assert len(gate_results) == 1
        assert "gate_1" in gate_results
        assert gate_results["gate_1"].node_type == "gate"
        
        collection_results = self.engine.get_results_by_node_type("collection")
        assert len(collection_results) == 1
        assert "collection_1" in collection_results
        assert collection_results["collection_1"].node_type == "collection"
    
    def test_get_successful_nodes_by_type(self):
        """测试获取指定类型中成功执行的节点"""
        # 创建成功和失败的节点
        success_logic = LogicNode("success_logic")
        success_logic.set_logic("x = 1")
        success_logic.add_tracked_variable("x")
        
        fail_logic = LogicNode("fail_logic")
        fail_logic.set_logic("raise Exception('test error')")
        
        gate_node = GateNode("gate_1")
        gate_node.set_condition("False")  # 会阻止执行
        
        # 添加到引擎
        self.engine.add_node(success_logic)
        self.engine.add_node(fail_logic)
        self.engine.add_node(gate_node)
        
        # 添加依赖关系
        self.engine.add_dependency(None, success_logic)
        self.engine.add_dependency(None, fail_logic)
        self.engine.add_dependency(None, gate_node)
        
        # 执行
        self.engine.execute()
        
        # 测试获取成功的逻辑节点
        successful_logic_nodes = self.engine.get_successful_nodes_by_type("logic")
        assert "success_logic" in successful_logic_nodes
        assert "fail_logic" not in successful_logic_nodes
        
        # 测试获取成功的门控节点
        successful_gate_nodes = self.engine.get_successful_nodes_by_type("gate")
        assert len(successful_gate_nodes) == 1  # 门控节点执行成功，只是should_continue为False
        assert "gate_1" in successful_gate_nodes
    
    def test_execution_summary_with_node_type(self):
        """测试执行摘要包含节点类型统计"""
        # 创建多个节点
        logic_node = LogicNode("logic_1")
        logic_node.set_logic("x = 1")
        logic_node.add_tracked_variable("x")
        
        gate_node = GateNode("gate_1")
        gate_node.set_condition("True")
        
        fail_logic = LogicNode("fail_logic")
        fail_logic.set_logic("raise Exception('test error')")
        
        # 添加到引擎
        self.engine.add_node(logic_node)
        self.engine.add_node(gate_node)
        self.engine.add_node(fail_logic)
        
        # 添加依赖关系
        self.engine.add_dependency(None, logic_node)
        self.engine.add_dependency(None, gate_node)
        self.engine.add_dependency(None, fail_logic)
        
        # 执行
        self.engine.execute()
        
        # 获取执行摘要
        summary = self.engine.get_execution_summary()
        
        # 检查节点类型统计
        assert "node_types_count" in summary
        type_counts = summary["node_types_count"]
        assert type_counts["logic"] == 2
        assert type_counts["gate"] == 1
        assert type_counts["start"] == 1
        
        # 检查按节点类型的执行结果统计
        assert "node_type_results" in summary
        node_type_results = summary["node_type_results"]
        
        # 检查逻辑节点的结果
        if "logic" in node_type_results:
            logic_results = node_type_results["logic"]
            assert logic_results["total"] == 2
            assert logic_results["successful"] == 1
            assert logic_results["failed"] == 1
        
        # 检查门控节点的结果
        if "gate" in node_type_results:
            gate_results = node_type_results["gate"]
            assert gate_results["total"] == 1
            assert gate_results["successful"] == 1
    
    def test_visualize_flow_includes_node_type(self):
        """测试可视化包含节点类型信息"""
        logic_node = LogicNode("test_logic")
        gate_node = GateNode("test_gate")
        
        self.engine.add_node(logic_node)
        self.engine.add_node(gate_node)
        
        flow_text = self.engine.visualize_flow()
        
        # 检查是否包含节点类型信息
        assert "test_logic (LogicNode) (logic)" in flow_text
        assert "test_gate (GateNode) (gate)" in flow_text
        
        # 检查是否包含节点类型统计
        assert "Node Types:" in flow_text
        assert "logic: 1" in flow_text
        assert "gate: 1" in flow_text
        assert "start: 1" in flow_text
    
    def test_json_export_import_with_node_type(self):
        """测试JSON导出导入包含node_type"""
        # 创建节点
        logic_node = LogicNode("test_logic")
        logic_node.set_logic("x = 1")
        logic_node.add_tracked_variable("x")
        
        gate_node = GateNode("test_gate")
        gate_node.set_condition("True")
        
        # 添加到引擎
        self.engine.add_node(logic_node)
        self.engine.add_node(gate_node)
        
        # 添加依赖关系
        self.engine.add_dependency(None, logic_node)
        self.engine.add_dependency(logic_node, gate_node)
        
        # 导出
        json_str = self.engine.export_to_json()
        
        # 检查JSON包含node_type
        assert '"node_type": "logic"' in json_str
        assert '"node_type": "gate"' in json_str
        
        # 导入
        new_engine = RuleEngine.import_from_json(json_str)
        
        # 检查导入后的节点类型
        logic_node_info = new_engine.get_node_info("test_logic")
        assert logic_node_info["node_type"] == "logic"
        
        gate_node_info = new_engine.get_node_info("test_gate")
        assert gate_node_info["node_type"] == "gate"
    
    def test_custom_node_type_override(self):
        """测试自定义节点类型覆盖"""
        class CustomLogicNode(LogicNode):
            def _get_node_type(self) -> str:
                return "custom_logic"
        
        custom_node = CustomLogicNode("custom_node")
        assert custom_node.node_type == "custom_logic"
        
        # 添加到引擎
        self.engine.add_node(custom_node)
        
        # 检查引擎能正确识别自定义类型
        custom_nodes = self.engine.get_nodes_by_type("custom_logic")
        assert len(custom_nodes) == 1
        assert "custom_node" in custom_nodes
        
        # 检查节点信息
        node_info = self.engine.get_node_info("custom_node")
        assert node_info["node_type"] == "custom_logic"
    
    def test_node_type_in_error_results(self):
        """测试错误结果也包含node_type"""
        # 创建一个会失败的逻辑节点
        fail_logic = LogicNode("fail_logic")
        fail_logic.set_logic("raise ValueError('test error')")
        
        self.engine.add_node(fail_logic)
        self.engine.add_dependency(None, fail_logic)
        
        # 执行
        results = self.engine.execute()
        
        # 检查失败的结果也包含node_type
        assert "fail_logic" in results
        result = results["fail_logic"]
        assert not result.success
        assert result.node_type == "logic"
        assert "test error" in result.error
    
    def test_node_type_in_engine_exception_results(self):
        """测试节点执行异常时 NodeResult 依然有 node_type"""
        logic_node = LogicNode("fail_logic")
        logic_node.set_logic("raise Exception('test error')")
        logic_node.add_tracked_variable("x")
        self.engine.add_node(logic_node)
        self.engine.add_dependency(None, logic_node)
        results = self.engine.execute()
        assert "fail_logic" in results
        result = results["fail_logic"]
        assert not result.success
        assert result.node_type == "logic"
        assert "test error" in result.error


if __name__ == "__main__":
    pytest.main([__file__]) 