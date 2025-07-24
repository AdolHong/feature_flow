"""
数据流管理 - 简化的依赖关系管理
"""

from typing import Dict, List, Any, Tuple
from .nodes import Node


class DataFlow:
    """数据流管理 - 简化的依赖关系管理"""
    
    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.dependencies: Dict[str, List[str]] = {}
    
    def add_node(self, node: Node):
        """添加节点到数据流"""
        self.nodes[node.name] = node
        self.dependencies[node.name] = []
    
    def add_dependency(self, source_node: str, target_node: str):
        """添加依赖关系 - target_node 依赖 source_node"""
        if source_node not in self.nodes or target_node not in self.nodes:
            raise ValueError(f"Node not found: {source_node} or {target_node}")
        
        # 添加依赖关系 - target_node 依赖 source_node
        if target_node not in self.dependencies:
            self.dependencies[target_node] = []
        
        if source_node not in self.dependencies[target_node]:
            self.dependencies[target_node].append(source_node)
    
    def get_execution_order(self) -> List[str]:
        """获取执行顺序（拓扑排序）"""
        return self._topological_sort()
    
    def _topological_sort(self) -> List[str]:
        """拓扑排序"""
        in_degree = {node: 0 for node in self.nodes}
        
        # 计算入度：node 依赖 deps，node 入度+1
        for node, deps in self.dependencies.items():
            for dep in deps:
                in_degree[node] += 1

        queue = [node for node, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            current = queue.pop(0)
            result.append(current)
            
            # 当前节点被移除后，所有依赖它的节点入度-1
            for child, deps in self.dependencies.items():
                if current in deps:
                    in_degree[child] -= 1
                    if in_degree[child] == 0:
                        queue.append(child)

        if len(result) != len(self.nodes):
            raise ValueError("Circular dependency detected")
        return result
    
    def get_node_dependencies(self, node_name: str) -> List[str]:
        """获取节点的依赖"""
        return self.dependencies.get(node_name, [])
    
    def get_node_dependents(self, node_name: str) -> List[str]:
        """获取依赖该节点的节点"""
        dependents = []
        for node, deps in self.dependencies.items():
            if node_name in deps:
                dependents.append(node)
        return dependents
    
    def remove_node(self, node_name: str):
        """删除节点"""
        if node_name in self.nodes:
            del self.nodes[node_name]
            
        # 从依赖关系中删除
        if node_name in self.dependencies:
            del self.dependencies[node_name]
        
        # 从其他节点的依赖中删除
        for deps in self.dependencies.values():
            if node_name in deps:
                deps.remove(node_name)
    
    def remove_dependency(self, source_node: str, target_node: str):
        """删除依赖关系"""
        if target_node in self.dependencies and source_node in self.dependencies[target_node]:
            self.dependencies[target_node].remove(source_node)
    
    def validate_structure(self) -> Tuple[bool, List[str]]:
        """验证数据结构"""
        errors = []
        
        # 1. 检查start_node是否存在
        if "start_node" not in self.nodes:
            errors.append("Missing start_node")
            return False, errors
        
        # 2. 计算所有与start_node连通的节点
        reachable = set()
        queue = ["start_node"]
        while queue:
            current = queue.pop(0)
            if current in reachable:
                continue
            reachable.add(current)
            # 找到所有依赖current的节点（即current是source，child是target）
            for child, deps in self.dependencies.items():
                if current in deps:
                    queue.append(child)
        
        # 3. 检查所有节点是否都可达
        for node_name in self.nodes:
            if node_name not in reachable:
                errors.append(f"Unreachable node (not connected to start_node): {node_name}")
        
        # 4. 检查循环依赖
        try:
            self.get_execution_order()
        except ValueError as e:
            errors.append(str(e))
        
        return len(errors) == 0, errors
    
    def __repr__(self):
        return f"DataFlow(nodes={list(self.nodes.keys())}, dependencies={len(self.dependencies)})" 