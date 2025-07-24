import pytest
from core.engine import RuleEngine
from core.nodes import LogicNode, CollectionNode

def test_collection_node_logic():
    """测试CollectionNode的逻辑处理功能"""
    engine = RuleEngine("collection_logic_test")
    
    # 创建上游节点
    logic1 = LogicNode("logic1")
    logic1.set_logic("result = 10")
    logic1.set_tracked_variables(["result"])
    
    logic2 = LogicNode("logic2")
    logic2.set_logic("result = 20")
    logic2.set_tracked_variables(["result"])
    
    # 创建CollectionNode
    collection = CollectionNode("collection")
    collection.add_expected_input_schema("result", "int")  # 添加schema
    collection.set_logic("""
# 处理收集到的数据
total = sum(data['result'] for data in collection.values())
count = len(collection)
average = total / count if count > 0 else 0
""")
    collection.set_tracked_variables(["total", "count", "average"])
    
    # 添加依赖关系，自动添加节点
    engine.add_dependency(None, logic1)
    engine.add_dependency(None, logic2)
    engine.add_dependency(logic1, collection)
    engine.add_dependency(logic2, collection)
    
    # 执行引擎
    results = engine.execute()
    
    # 验证结果
    assert results["logic1"].success
    assert results["logic2"].success
    assert results["collection"].success
    
    # 验证collection的输出
    collection_data = results["collection"].data
    assert collection_data["total"] == 30  # 10 + 20
    assert collection_data["count"] == 2
    assert collection_data["average"] == 15.0  # 30 / 2

def test_collection_node_empty_logic():
    """测试CollectionNode的空逻辑处理"""
    engine = RuleEngine("collection_empty_logic_test")
    
    # 创建上游节点
    logic1 = LogicNode("logic1")
    logic1.set_logic("result = 10")
    logic1.set_tracked_variables(["result"])
    
    logic2 = LogicNode("logic2")
    logic2.set_logic("result = 20")
    logic2.set_tracked_variables(["result"])
    
    # 创建CollectionNode，不设置逻辑
    collection = CollectionNode("collection")
    collection.add_expected_input_schema("result", "int")  # 添加schema
    # 不设置logic_code
    
    # 添加依赖关系，自动添加节点
    engine.add_dependency(None, logic1)
    engine.add_dependency(None, logic2)
    engine.add_dependency(logic1, collection)
    engine.add_dependency(logic2, collection)
    
    # 执行引擎
    results = engine.execute()
    
    # 验证结果
    assert results["logic1"].success
    assert results["logic2"].success
    assert results["collection"].success
    
    # 验证collection的输出（应该返回原始收集数据）
    collection_data = results["collection"].data
    assert "collected_data" in collection_data
    collected_items = collection_data["collected_data"]
    assert len(collected_items) == 2
    
    # 验证收集的数据结构
    for node_name, data in collected_items.items():
        assert node_name in ["logic1", "logic2"]
        assert "result" in data
        assert data["result"] in [10, 20] 