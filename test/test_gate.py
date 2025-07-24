#!/usr/bin/env python3
"""
测试GateNode逻辑
"""

from core.engine import RuleEngine
from core.nodes import LogicNode, GateNode

def test_gate_node():
    """测试GateNode的逻辑"""
    print("=== 测试GateNode逻辑 ===")
    
    # 测试1: 有should_continue且为True
    print("\n--- 测试1: should_continue = True ---")
    engine1 = RuleEngine("test1")
    
    gate1 = GateNode("gate1")
    gate1.set_condition("True")
    
    logic1 = LogicNode("logic1")
    logic1.set_logic("output = ['logic1', 100, 'A']")
    logic1.set_tracked_variables(["output"])
    
    engine1.add_dependency(None, gate1)
    engine1.add_dependency(gate1, logic1)
    
    # 执行引擎
    results1 = engine1.execute()
    
    # 验证结果
    assert results1["gate1"].success
    assert results1["gate1"].data["should_continue"] == True
    assert results1["logic1"].success
    
    print("✅ 测试1通过")
    
    # 测试2: 有should_continue且为False
    print("\n--- 测试2: should_continue = False ---")
    engine2 = RuleEngine("test2")
    
    gate2 = GateNode("gate2")
    gate2.set_condition("False")
    
    logic2 = LogicNode("logic2")
    logic2.set_logic("output = ['logic2', 200, 'B']")
    logic2.set_tracked_variables(["output"])
    
    engine2.add_dependency(None, gate2)
    engine2.add_dependency(gate2, logic2)
    
    # 执行引擎
    results2 = engine2.execute()
    
    # 验证结果
    assert results2["gate2"].success
    assert results2["gate2"].data["should_continue"] == False
    # logic2应该被阻止执行
    assert not results2["logic2"].success
    assert "blocked" in results2["logic2"].status
    
    print("✅ 测试2通过")
    
    # 测试3: 条件表达式
    print("\n--- 测试3: 条件表达式 ---")
    engine3 = RuleEngine("test3")
    
    logic3 = LogicNode("logic3")
    logic3.set_logic("x = 10; y = 20")
    logic3.set_tracked_variables(["x", "y"])
    
    gate3 = GateNode("gate3")
    gate3.set_condition("x > 5 and y < 30")
    
    engine3.add_dependency(None, logic3)
    engine3.add_dependency(logic3, gate3)
    
    # 执行引擎
    results3 = engine3.execute()
    
    # 验证结果
    assert results3["logic3"].success
    assert results3["gate3"].success
    assert results3["gate3"].data["should_continue"] == True
    
    print("✅ 测试3通过")
    
    print("\n=== 所有GateNode测试通过 ===")

if __name__ == "__main__":
    test_gate_node() 