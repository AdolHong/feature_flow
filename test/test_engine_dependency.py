"""
测试引擎依赖关系管理功能
"""

import pytest
from core.engine import RuleEngine
from core.nodes import LogicNode, GateNode, CollectionNode


class TestEngineDependency:
    """测试引擎依赖关系管理"""
    
    def test_add_dependency_auto_add_nodes(self):
        """测试add_dependency自动添加节点"""
        engine = RuleEngine("test_engine")
        
        # 创建节点
        logic1 = LogicNode("logic1")
        logic2 = LogicNode("logic2")
        output = CollectionNode("output")
        
        # 直接添加依赖关系，应该自动添加节点
        engine.add_dependency(None, logic1)
        engine.add_dependency(None, logic2)
        engine.add_dependency(logic1, output)
        engine.add_dependency(logic2, output)
        
        # 验证节点已被添加
        assert engine.get_node("logic1") is logic1
        assert engine.get_node("logic2") is logic2
        assert engine.get_node("output") is output
        assert engine.get_node("start_node") is not None
        
        # 验证依赖关系
        assert "start_node" in engine.get_node_dependencies("logic1")
        assert "start_node" in engine.get_node_dependencies("logic2")
        assert "logic1" in engine.get_node_dependencies("output")
        assert "logic2" in engine.get_node_dependencies("output")
    
    def test_add_dependency_node_name_conflict(self):
        """测试节点名称冲突检测"""
        engine = RuleEngine("test_engine")
        
        # 创建两个同名但不同的节点
        logic1 = LogicNode("logic1")
        logic2 = LogicNode("logic1")  # 同名但不同对象
        
        # 先添加一个节点
        engine.add_node(logic1)
        
        # 尝试添加依赖关系，应该检测到冲突
        with pytest.raises(ValueError) as exc_info:
            engine.add_dependency(None, logic2)
        
        assert "节点名称冲突" in str(exc_info.value)
        assert "logic1" in str(exc_info.value)
    
    def test_add_dependency_string_node_names(self):
        """测试使用字符串节点名称"""
        engine = RuleEngine("test_engine")
        
        # 创建节点
        logic1 = LogicNode("logic1")
        logic2 = LogicNode("logic2")
        
        # 直接添加依赖关系，自动添加节点
        engine.add_dependency(None, logic1)
        engine.add_dependency(logic1, logic2)
        
        # 验证依赖关系
        assert "start_node" in engine.get_node_dependencies("logic1")
        assert "logic1" in engine.get_node_dependencies("logic2")
    
    def test_add_dependency_mixed_node_types(self):
        """测试混合使用节点对象"""
        engine = RuleEngine("test_engine")
        
        # 创建节点
        logic1 = LogicNode("logic1")
        gate1 = GateNode("gate1")
        output = CollectionNode("output")
        
        # 直接添加依赖关系，自动添加节点
        engine.add_dependency(None, logic1)  # 节点对象
        engine.add_dependency(logic1, gate1)  # 节点对象
        engine.add_dependency(gate1, output)  # 节点对象
        
        # 验证节点和依赖关系
        assert engine.get_node("logic1") is logic1
        assert engine.get_node("gate1") is gate1
        assert engine.get_node("output") is output
        
        assert "start_node" in engine.get_node_dependencies("logic1")
        assert "logic1" in engine.get_node_dependencies("gate1")
        assert "gate1" in engine.get_node_dependencies("output")
    
    def test_add_dependency_existing_nodes(self):
        """测试对已存在节点添加依赖关系"""
        engine = RuleEngine("test_engine")
        
        # 先添加节点
        logic1 = LogicNode("logic1")
        logic2 = LogicNode("logic2")
        engine.add_node(logic1)
        engine.add_node(logic2)
        
        # 添加依赖关系，不应该重复添加节点
        engine.add_dependency(logic1, logic2)
        
        # 验证节点仍然是同一个对象
        assert engine.get_node("logic1") is logic1
        assert engine.get_node("logic2") is logic2
        
        # 验证依赖关系
        assert "logic1" in engine.get_node_dependencies("logic2")
    
    def test_add_dependency_start_node_handling(self):
        """测试start_node的特殊处理"""
        engine = RuleEngine("test_engine")
        
        logic1 = LogicNode("logic1")
        
        # 使用None作为源节点，应该自动使用start_node
        engine.add_dependency(None, logic1)
        
        # 验证start_node被创建
        start_node = engine.get_node("start_node")
        assert start_node is not None
        assert start_node.name == "start_node"
        
        # 验证依赖关系
        assert "start_node" in engine.get_node_dependencies("logic1")
    
    def test_add_dependency_complex_flow(self):
        """测试复杂的数据流"""
        engine = RuleEngine("test_engine")
        
        # 创建复杂的节点网络
        logic1 = LogicNode("logic1")
        logic2 = LogicNode("logic2")
        gate1 = GateNode("gate1")
        collection1 = CollectionNode("collection1")
        output = CollectionNode("output")
        
        # 添加依赖关系，自动构建网络
        engine.add_dependency(None, logic1)
        engine.add_dependency(None, logic2)
        engine.add_dependency(logic1, gate1)
        engine.add_dependency(logic2, gate1)
        engine.add_dependency(gate1, collection1)
        engine.add_dependency(collection1, output)
        
        # 验证所有节点都被添加
        assert engine.get_node("logic1") is logic1
        assert engine.get_node("logic2") is logic2
        assert engine.get_node("gate1") is gate1
        assert engine.get_node("collection1") is collection1
        assert engine.get_node("output") is output
        
        # 验证执行顺序
        execution_order = engine.get_execution_order()
        assert "start_node" in execution_order
        assert "logic1" in execution_order
        assert "logic2" in execution_order
        assert "gate1" in execution_order
        assert "collection1" in execution_order
        assert "output" in execution_order
        
        # 验证依赖关系
        assert "start_node" in engine.get_node_dependencies("logic1")
        assert "start_node" in engine.get_node_dependencies("logic2")
        assert "logic1" in engine.get_node_dependencies("gate1")
        assert "logic2" in engine.get_node_dependencies("gate1")
        assert "gate1" in engine.get_node_dependencies("collection1")
        assert "collection1" in engine.get_node_dependencies("output")


if __name__ == '__main__':
    pytest.main([__file__]) 