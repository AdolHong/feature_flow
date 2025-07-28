"""
规则引擎核心类 - 简化版本
删除OutputNode，只支持start、logic、gate、collection四种节点
所有输出通过tracked_variables/flow_context维护
支持全局schema管理
"""

import logging
import json
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime
from .nodes import Node, NodeResult, StartNode, GateNode, LogicNode, CollectionNode, parse_dynamic_parameters
from .data_flow import DataFlow


class RuleEngine:
    """规则引擎核心类 - 简化版本"""
    
    def __init__(self, name: str = "default"):
        self.name = name
        self.data_flow = DataFlow()
        self.context: Dict[str, Any] = {}
        self.placeholders: Dict[str, Any] = {}
        self.job_date: str = None
        self.execution_results: Dict[str, NodeResult] = {}
        self.logger = logging.getLogger(f"RuleEngine.{name}")
        
        # 默认添加start_node
        self.start_node = StartNode("start_node")
        self.data_flow.add_node(self.start_node)
        
        
    # ==================== 节点管理 ====================
    def add_node(self, node: Node):
        """添加节点到引擎"""
        self.data_flow.add_node(node)
        self.logger.info(f"Added node: {node.name} ({node.__class__.__name__})")
    
    def get_node(self, node_name: str) -> Optional[Node]:
        """获取节点"""
        return self.data_flow.nodes.get(node_name)
    
    def get_all_nodes(self) -> Dict[str, Node]:
        """获取所有节点"""
        return self.data_flow.nodes.copy()
    
    # ==================== 依赖关系管理 ====================
    def add_dependency(self, source_node: Optional[Node], target_node: Node):
        """添加节点依赖关系，自动添加节点"""
        # 处理源节点
        if source_node is None:
            source_node = self.get_node("start_node")

        
        # 检查节点名称冲突
        if source_node:
            existing_source = self.get_node(source_node.name)
            if existing_source and existing_source is not source_node:
                raise ValueError(f"节点名称冲突: '{source_node.name}' 已存在但节点对象不同")
            elif existing_source is None:
                self.data_flow.add_node(source_node)
        
        if target_node:
            existing_target = self.get_node(target_node.name)
            if existing_target and existing_target is not target_node:
                raise ValueError(f"节点名称冲突: '{target_node.name}' 已存在但节点对象不同")
            elif existing_target is None:
                self.data_flow.add_node(target_node)

        # 添加依赖关系
        self.data_flow.add_dependency(source_node.name, target_node.name)
        self.logger.info(f"Added dependency: {source_node.name} -> {target_node.name}")
    
    def get_node_dependencies(self, node_name: str) -> List[str]:
        """获取节点的依赖"""
        return self.data_flow.get_node_dependencies(node_name)
    
    def get_node_dependents(self, node_name: str) -> List[str]:
        """获取依赖该节点的节点"""
        return self.data_flow.get_node_dependents(node_name)
    
    # ==================== 执行顺序管理 ====================
    def get_execution_order(self) -> List[str]:
        """获取执行顺序"""
        return self.data_flow.get_execution_order()
    
    # ==================== 上下文管理 ====================
    def set_context(self, context: Dict[str, Any]):
        """设置执行上下文"""
        self.context = context
        self.logger.info(f"Set context with {len(context)} items")
    
    def get_context(self) -> Dict[str, Any]:
        """获取执行上下文"""
        return self.context.copy()
    
    def get_placeholders(self) -> Dict[str, Any]:
        """获取占位符"""
        return self.placeholders.copy()
    
    def get_job_date(self) -> str:
        """获取作业日期"""
        return self.job_date
    
    # ==================== 验证 ====================
    def validate(self) -> Tuple[bool, List[str]]:
        """验证规则引擎配置"""
        return self.data_flow.validate_structure()
    
    # ==================== 执行逻辑 ====================
    def execute(self, job_date: str = None, placeholders: Dict[str, Any] = None, variables: Dict[str, Any] = None) -> Dict[str, NodeResult]:
        """执行规则引擎"""
        # 设置实例变量
        self.job_date = job_date
        self.placeholders = placeholders or {}
        
        # 构建执行上下文
        execution_context = {}
        
        # 添加job_date
        if job_date:
            execution_context['job_date'] = job_date
        
        # 添加placeholders和variables
        if placeholders:
            execution_context.update(placeholders)
        if variables:
            execution_context.update(variables)
        
        self.context = execution_context
        
        # 验证配置
        is_valid, errors = self.validate()
        if not is_valid:
            raise ValueError(f"Rule engine validation failed: {errors}")
        
        self.logger.info(f"Starting execution of rule engine: {self.name}")
        if job_date:
            self.logger.info(f"Job date: {job_date}")
        
        # 获取执行顺序
        execution_order = self.get_execution_order()
        self.logger.info(f"Execution order: {execution_order}")
        
        # 按顺序执行节点
        for node_name in execution_order:
            try:
                result = self._execute_node(node_name)
                self.execution_results[node_name] = result
                
                if result.success:
                    self.logger.info(f"Node {node_name} executed successfully")
                else:
                    self.logger.error(f"Node {node_name} failed: {result.error}")
                    
            except Exception as e:
                error_result = NodeResult(success=False, error=str(e))
                self.execution_results[node_name] = error_result
                self.logger.error(f"Node {node_name} execution error: {e}")
        
        self.logger.info(f"Rule engine execution completed")
        return self.execution_results
    
    def _execute_node(self, node_name: str) -> NodeResult:
        """执行单个节点"""
        node = self.data_flow.nodes[node_name]
        
        # 检查是否被GateNode阻止执行
        for dep_node_name in self.get_node_dependencies(node_name):
            dep_node = self.data_flow.nodes[dep_node_name]
            if isinstance(dep_node, GateNode):
                dep_result = self.execution_results.get(dep_node_name)
                if dep_result and dep_result.success:
                    if hasattr(dep_node, 'should_continue') and not dep_node.should_continue:
                        # 被GateNode阻止，跳过执行
                        return NodeResult(success=False, error=f"Execution blocked by gate node {dep_node_name}", status="blocked")
        
        # 检查：如果有依赖节点且所有依赖节点都未成功，则本节点被阻断
        dep_names = self.get_node_dependencies(node_name)
        if dep_names:
            all_blocked_or_failed = True
            for dep in dep_names:
                dep_result = self.execution_results.get(dep)
                if dep_result and dep_result.success:
                    all_blocked_or_failed = False
                    break
            if all_blocked_or_failed:
                return NodeResult(success=False, error=f"All dependencies failed or blocked for node {node_name}", status="blocked")
        
        # 获取节点输入数据
        inputs = self._get_node_inputs(node_name)
        
        # 设置节点输入
        for input_name, input_data in inputs.items():
            node.add_input(input_name, input_data)
        
        # 执行节点
        result = node.execute(self.context, self.job_date, self.placeholders)
        
        return result
    
    def _get_node_inputs(self, node_name: str) -> Dict[str, Any]:
        """获取节点的输入数据"""
        node = self.data_flow.nodes[node_name]
        inputs = {}
        
        # 从依赖节点获取数据
        for dep_node_name in self.get_node_dependencies(node_name):
            dep_node = self.data_flow.nodes[dep_node_name]
            
            # 检查依赖节点的执行结果，只传递成功执行的节点的数据
            dep_result = self.execution_results.get(dep_node_name)
            if dep_result and not dep_result.success:
                # 如果依赖节点执行失败，跳过该节点
                continue
            
            # 检查门控节点的should_continue
            if isinstance(dep_node, GateNode):
                if hasattr(dep_node, 'should_continue') and not dep_node.should_continue:
                    # 门控节点决定不继续，跳过后续节点
                    continue
            
            # 传递节点对象，以便访问flow_context
            inputs[f"__node_{dep_node_name}"] = dep_node
        
        # 合并上下文数据到输入，确保全局变量可用
        inputs.update(self.context)
        
        return inputs
    
    # ==================== 结果查询 ====================
    def get_node_result(self, node_name: str) -> Optional[NodeResult]:
        """获取节点执行结果"""
        return self.execution_results.get(node_name)
    
    def get_final_outputs(self) -> Dict[str, Any]:
        """获取最终输出结果"""
        outputs = {}
        for node_name, result in self.execution_results.items():
            if result.success and hasattr(result.data, 'get'):
                if isinstance(result.data, dict):
                    outputs[node_name] = result.data
                else:
                    outputs[node_name] = result.data
        return outputs
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """获取执行摘要"""
        total_nodes = len(self.data_flow.nodes)
        successful_nodes = sum(1 for result in self.execution_results.values() if result.success)
        failed_nodes = sum(1 for result in self.execution_results.values() if not result.success and result.status == "failed")
        blocked_nodes = sum(1 for result in self.execution_results.values() if not result.success and result.status == "blocked")
        executed_nodes = sum(1 for result in self.execution_results.values() if result.status == "executed")
        
        return {
            'total_nodes': total_nodes,
            'successful_nodes': successful_nodes,
            'failed_nodes': failed_nodes,
            'blocked_nodes': blocked_nodes,
            'executed_nodes': executed_nodes,
            'success_rate': successful_nodes / (total_nodes - blocked_nodes) if total_nodes > 0 else 0,
            'failed_rate': failed_nodes / (total_nodes - blocked_nodes) if total_nodes > 0 else 0,
            'execution_order': self.get_execution_order(),
            'failed_nodes_list': [
                node_name for node_name, result in self.execution_results.items() 
                if not result.success and result.status == "failed"
            ],
            'blocked_nodes_list': [
                node_name for node_name, result in self.execution_results.items() 
                if not result.success and result.status == "blocked"
            ]
        }
    
    # ==================== 状态管理 ====================
    def reset(self):
        """重置引擎状态"""
        self.execution_results.clear()
        self.context.clear()
        self.placeholders.clear()
        self.job_date = None
        # 清空所有节点的flow_context
        for node in self.data_flow.nodes.values():
            if hasattr(node, 'clear_flow_context'):
                node.clear_flow_context()
        self.logger.info("Rule engine reset")
    
    def clear_execution_results(self):
        """清除执行结果"""
        self.execution_results.clear()
    
    # ==================== Flow Context 管理 ====================
    def set_node_tracked_variables(self, node_name: str, variables: List[str]):
        """设置节点要跟踪的变量"""
        node = self.get_node(node_name)
        if node:
            node.set_tracked_variables(variables)
            self.logger.info(f"Set tracked variables for node {node_name}: {variables}")
        else:
            self.logger.warning(f"Node {node_name} not found")
    
    def add_node_tracked_variable(self, node_name: str, variable: str):
        """为节点添加单个要跟踪的变量"""
        node = self.get_node(node_name)
        if node:
            node.add_tracked_variable(variable)
            self.logger.info(f"Added tracked variable {variable} for node {node_name}")
        else:
            self.logger.warning(f"Node {node_name} not found")
    
    def set_node_expected_input_schema(self, node_name: str, schema: Dict[str, str]):
        """设置节点的期望输入schema"""
        node = self.get_node(node_name)
        if node:
            node.set_expected_input_schema(schema)
            self.logger.info(f"Set expected input schema for node {node_name}: {schema}")
        else:
            self.logger.warning(f"Node {node_name} not found")
    
    def add_node_expected_input_schema(self, node_name: str, variable: str, schema: str):
        """为节点添加单个期望的输入schema"""
        node = self.get_node(node_name)
        if node:
            node.add_expected_input_schema(variable, schema)
            self.logger.info(f"Added expected input schema for node {node_name}: {variable} -> {schema}")
        else:
            self.logger.warning(f"Node {node_name} not found")
    
    def get_node_flow_context(self, node_name: str) -> Dict[str, Tuple[Any, str, datetime]]:
        """获取节点的flow_context"""
        node = self.get_node(node_name)
        if node and hasattr(node, 'get_flow_context'):
            return node.get_flow_context()
        return {}
    
    def get_node_flow_context_values(self, node_name: str) -> Dict[str, Any]:
        """获取节点的flow_context中的值（不包含元数据）"""
        node = self.get_node(node_name)
        if node and hasattr(node, 'get_flow_context_values'):
            return node.get_flow_context_values()
        return {}
    
    def get_all_nodes_flow_context(self) -> Dict[str, Dict[str, Tuple[Any, str, datetime]]]:
        """获取所有节点的flow_context"""
        flow_contexts = {}
        for node_name, node in self.data_flow.nodes.items():
            if hasattr(node, 'get_flow_context'):
                flow_contexts[node_name] = node.get_flow_context()
        return flow_contexts
    
    def get_all_nodes_flow_context_values(self) -> Dict[str, Dict[str, Any]]:
        """获取所有节点的flow_context中的值（不包含元数据）"""
        flow_context_values = {}
        for node_name, node in self.data_flow.nodes.items():
            if hasattr(node, 'get_flow_context_values'):
                flow_context_values[node_name] = node.get_flow_context_values()
        return flow_context_values
    
    def clear_all_nodes_flow_context(self):
        """清空所有节点的flow_context"""
        for node in self.data_flow.nodes.values():
            if hasattr(node, 'clear_flow_context'):
                node.clear_flow_context()
        self.logger.info("Cleared all nodes flow_context")
    
    # ==================== 信息查询 ====================
    def get_node_info(self, node_name: str) -> Dict[str, Any]:
        """获取节点信息"""
        if node_name not in self.data_flow.nodes:
            return {}
        
        node = self.data_flow.nodes[node_name]
        dependencies = self.get_node_dependencies(node_name)
        dependents = self.get_node_dependents(node_name)
        
        node_info = {
            'name': node_name,
            'type': node.__class__.__name__,
            'dependencies': dependencies,
            'dependents': dependents,
            'inputs': node.inputs,
            'tracked_variables': node.tracked_variables,
            'expected_input_schema': node.expected_input_schema
        }
        
        # 添加flow_context相关信息
        if hasattr(node, 'get_flow_context'):
            node_info['flow_context'] = node.get_flow_context()
            node_info['flow_context_values'] = node.get_flow_context_values()
        
        # 根据节点类型添加特定信息
        if isinstance(node, LogicNode):
            node_info['logic_code'] = node.logic_code
        elif isinstance(node, GateNode):
            node_info['condition'] = node.condition
            node_info['should_continue'] = node.should_continue
        elif isinstance(node, CollectionNode):
            node_info['logic_code'] = node.logic_code
            node_info['collected_data'] = node.collected_data
        
        return node_info
    
    def get_engine_info(self) -> Dict[str, Any]:
        """获取引擎信息"""
        return {
            'name': self.name,
            'total_nodes': len(self.data_flow.nodes),
            'total_dependencies': sum(len(deps) for deps in self.data_flow.dependencies.values()),
            'execution_order': self.get_execution_order(),
            'context_size': len(self.context),
            'placeholders_size': len(self.placeholders),
            'job_date': self.job_date,
            'results_size': len(self.execution_results)
        }
    
    # ==================== 可视化 ====================
    def visualize_flow(self) -> str:
        """可视化数据流（简单文本格式）"""
        lines = [f"Rule Engine: {self.name}", "=" * 50]
        

        
        # 节点信息
        lines.append("\nNodes:")
        for node_name, node in self.data_flow.nodes.items():
            lines.append(f"  {node_name} ({node.__class__.__name__})")
            if node.tracked_variables:
                lines.append(f"    Tracked: {node.tracked_variables}")
            if node.expected_input_schema:
                lines.append(f"    Expected Input: {node.expected_input_schema}")
        
        # 依赖关系
        lines.append("\nDependencies:")
        for node_name, deps in self.data_flow.dependencies.items():
            if deps:
                lines.append(f"  {node_name} <- {', '.join(deps)}")
        
        # 执行顺序
        try:
            execution_order = self.get_execution_order()
            lines.append(f"\nExecution Order:")
            lines.append(f"  {' -> '.join(execution_order)}")
        except ValueError as e:
            lines.append(f"\nExecution Order: ERROR - {e}")
        
        return "\n".join(lines)
    
    # ==================== JSON导入导出 ====================
    def export_to_json(self) -> str:
        """导出规则引擎配置为JSON字符串"""
        config = {
            'name': self.name,
            'nodes': {},
            'dependencies': self.data_flow.dependencies
        }
        
        # 导出节点配置（排除start_node）
        for node_name, node in self.data_flow.nodes.items():
            if node_name == 'start_node':
                continue
                
            node_config = {
                'type': node.__class__.__name__,
                'name': node.name,
                'tracked_variables': node.tracked_variables,
                'expected_input_schema': node.expected_input_schema
            }
            
            # 根据节点类型添加特定配置
            if isinstance(node, LogicNode):
                node_config['logic_code'] = node.logic_code
            elif isinstance(node, GateNode):
                node_config['condition'] = node.condition
            elif isinstance(node, CollectionNode):
                node_config['logic_code'] = node.logic_code
            
            config['nodes'][node_name] = node_config
        
        return json.dumps(config, ensure_ascii=True)
    
    @staticmethod
    def import_from_json(json_str: str) -> 'RuleEngine':
        """从JSON字符串创建新的规则引擎实例"""
        config = json.loads(json_str)
        
        # 创建新的引擎实例
        engine_name = config.get('name', 'default')
        engine = RuleEngine(engine_name)
        
        # 添加所有节点
        for node_name, node_config in config.get('nodes', {}).items():
            node_type = node_config['type']
            
            if node_type == 'LogicNode':
                node = LogicNode(node_name)
                if 'logic_code' in node_config:
                    node.set_logic(node_config['logic_code'])
            elif node_type == 'GateNode':
                node = GateNode(node_name)
                if 'condition' in node_config:
                    node.set_condition(node_config['condition'])
            elif node_type == 'CollectionNode':
                node = CollectionNode(node_name)
                if 'logic_code' in node_config:
                    node.set_logic(node_config['logic_code'])
            else:
                continue
            
            # 恢复tracked_variables和expected_input_schema
            if 'tracked_variables' in node_config:
                node.set_tracked_variables(node_config['tracked_variables'])
            if 'expected_input_schema' in node_config:
                node.set_expected_input_schema(node_config['expected_input_schema'])
            
            engine.data_flow.add_node(node)
        
        # 恢复依赖关系
        for target, sources in config.get('dependencies', {}).items():
            for source in sources:
                if source in engine.data_flow.nodes and target in engine.data_flow.nodes:
                    engine.data_flow.add_dependency(source, target)
        
        return engine
    
    def save_to_file(self, file_path: str):
        """保存规则引擎配置到文件"""
        json_str = self.export_to_json()
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json_str)
        self.logger.info(f"Configuration saved to: {file_path}")
    
    @staticmethod
    def load_from_file(file_path: str) -> 'RuleEngine':
        """从文件创建新的规则引擎实例"""
        with open(file_path, 'r', encoding='utf-8') as f:
            json_str = f.read()
        engine = RuleEngine.import_from_json(json_str)
        engine.logger.info(f"Configuration loaded from: {file_path}")
        return engine
    
    def __repr__(self):
        return f"RuleEngine(name='{self.name}', nodes={len(self.data_flow.nodes)})" 