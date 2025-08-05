"""
规则引擎节点定义 - 简化版本
只包含四种节点：start、logic、gate、collection
所有输出通过tracked_variables/flow_context维护
"""

import io
import pandas as pd
from contextlib import redirect_stdout
from typing import Any, Dict, List, Optional, Tuple
from abc import ABC, abstractmethod
from datetime import datetime
import json
import random
import string

# 添加utils目录到路径
from utils.datetime_parser import parse_datetime
from type.validator import DataValidator

from core.json_helper import UniversalEncoder


def generate_random_name(length=8):
    """
    生成一个指定长度的随机字符串，包含大小写字母和数字。
    """
    characters = string.ascii_letters + string.digits
    random_name = ''.join(random.choice(characters) for _ in range(length))
    return random_name


class NodeResult:
    """节点执行结果"""
    
    def __init__(self, success: bool, data: Any = None, text_output: str = None, error: Optional[str] = None, status: str = "executed", node_type: str = None):
        self.success = success
        self.data = data
        self.text_output = text_output
        self.error = error
        self.status = status  # "executed", "blocked", "failed", "skipped"
        self.node_type = node_type  # 节点类型：start, logic, gate, collection
    
    def __repr__(self):
        return f"NodeResult(success={self.success}, status={self.status}, node_type={self.node_type}, data={self.data}, text_output={self.text_output}, error={self.error})"


class Node(ABC):
    """节点基类"""
    
    def __init__(self, name: str):
        self.name = name
        self.inputs: Dict[str, Any] = {}
        # 初始化flow_context - 所有输出都通过这里维护
        self.flow_context: Dict[str, Tuple[Any, str, datetime]] = {}
        # 指定要跟踪的变量名列表（即输出变量）
        self.tracked_variables: List[str] = []
        # 期望的输入schema - 用于校验上游变量
        self.expected_input_schema: Dict[str, str] = {}
        # 节点类型
        self.node_type: str = self._get_node_type()
    
    def _get_node_type(self) -> str:
        """获取节点类型，子类可以重写此方法"""
        class_name = self.__class__.__name__
        if class_name == "StartNode":
            return "start"
        elif class_name == "LogicNode":
            return "logic"
        elif class_name == "GateNode":
            return "gate"
        elif class_name == "CollectionNode":
            return "collection"
        else:
            return "unknown"
    
    @abstractmethod
    def execute(self, context: Dict[str, Any], job_date: str = None, placeholders: Dict[str, Any] = None) -> NodeResult:
        """执行节点逻辑"""
        pass
    
    def add_input(self, name: str, value: Any):
        """添加输入数据"""
        self.inputs[name] = value
    
    def set_tracked_variables(self, variables: List[str]):
        """设置要跟踪的变量名列表（输出变量）"""
        self.tracked_variables = variables
    
    def add_tracked_variable(self, variable: str):
        """添加单个要跟踪的变量（输出变量）"""
        if variable not in self.tracked_variables:
            self.tracked_variables.append(variable)
    
    def set_expected_input_schema(self, schema: Dict[str, str]):
        """设置期望的输入schema"""
        self.expected_input_schema = schema
    
    def add_expected_input_schema(self, variable: str, schema: str):
        """添加单个期望的输入schema"""
        self.expected_input_schema[variable] = schema
    
    def update_flow_context(self, variable_name: str, value: Any, source_node: str = None, context: Dict[str, Any] = None):
        """更新flow_context，如果变量已存在且时间更新，则更新"""
        current_time = datetime.now()
        source_node_name = source_node if source_node else self.name
                
        # 方法1: 严格验证 - 如果无法序列化则抛出异常
        try:
            # 若非pandas dataframe, 必须校验其是否能序列化;  
            # pandas dataframe 需要单独处理
            json.dumps(value, cls=UniversalEncoder)
        except (TypeError, ValueError) as e:
            raise ValueError(f"变量 '{variable_name}' 无法序列化为JSON: {value}, 错误: {e}")
        
        if variable_name in self.flow_context:
            # 如果变量已存在，比较时间戳
            existing_value, existing_node, existing_time = self.flow_context[variable_name]
            if current_time > existing_time:
                self.flow_context[variable_name] = (value, source_node_name, current_time)
        else:
            # 如果变量不存在，直接添加
            self.flow_context[variable_name] = (value, source_node_name, current_time)
    
    def merge_flow_context_from_inputs(self):
        """从输入中合并flow_context，并进行校验"""
        for input_name, input_data in self.inputs.items():
            # 跳过以__node_开头的特殊输入（这些是节点对象）
            if input_name.startswith("__node_"):
                # 如果输入是节点对象且有flow_context
                if hasattr(input_data, 'flow_context') and isinstance(input_data.flow_context, dict):
                    for var_name, (var_value, var_node, var_time) in input_data.flow_context.items():
                        # 检查是否有schema限制
                        if var_name in self.expected_input_schema:
                            # input节点需要检查，collection节点这一步不需要检查，在collection节点中检查
                            if isinstance(self, CollectionNode):
                                continue
                            schema_str = self.expected_input_schema[var_name]
                            # 创建校验器并校验
                            validator = DataValidator.from_string(schema_str)
                            if not validator.validate(var_value): 
                                raise ValueError(f"Input variable {var_name} validation failed. Expected: {schema_str}, Got: {type(var_value).__name__}")
                        # 校验通过或无schema限制，继承变量（取最新的）
                        self.update_flow_context(var_name, var_value, var_node)
    
    def get_flow_context(self) -> Dict[str, Tuple[Any, str, datetime]]:
        """获取flow_context的副本"""
        return self.flow_context.copy()
    
    def get_flow_context_values(self) -> Dict[str, Any]:
        """获取flow_context中的值（不包含元数据）"""
        return {var_name: value for var_name, (value, _, _) in self.flow_context.items()}
    
    def clear_flow_context(self):
        """清空flow_context"""
        self.flow_context.clear()
    
    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"


class StartNode(Node):
    """开始节点 - 不做任何事情，逻辑的开始"""
    
    def __init__(self, name: str = "start_node"):
        super().__init__(name)
    
    def execute(self, context: Dict[str, Any], job_date: str = None, placeholders: Dict[str, Any] = None) -> NodeResult:
        """执行开始节点，不执行任何功能"""
        return NodeResult(success=True, data={}, node_type=self.node_type)


class GateNode(Node):
    """门控节点 - 判断数据流是否往下执行"""
    
    def __init__(self, name: Optional[str] = None, condition: Optional[str] = None):
        if name is None:
            name = f"gate_{generate_random_name()}"

        super().__init__(name)
        self.condition = "False" if condition is None else condition
        self.should_continue = False
    
    def set_condition(self, condition: str):
        """设置判断条件，例如: x == 1, y > 10"""
        self.condition = condition
    
    def execute(self, context: Dict[str, Any], job_date: str = None, placeholders: Dict[str, Any] = None) -> NodeResult:
        """执行门控节点"""
        if not self.condition:
            return NodeResult(success=False, error="No condition provided", node_type=self.node_type)
        
        # 合并来自上游节点的flow_context
        self.merge_flow_context_from_inputs()
        
        # 创建输出捕获
        text_output = io.StringIO()
        try:
            # 创建本地变量空间，包含输入数据、上下文和flow_context
            local_vars = {**self.inputs, **context, **self.get_flow_context_values()}
            with redirect_stdout(text_output):
                # 解析动态参数
                parsed_condition = parse_dynamic_parameters(self.condition, job_date, placeholders)
                # 执行条件判断
                self.should_continue = eval(parsed_condition, {}, local_vars)
            return NodeResult(success=True, data={'should_continue': self.should_continue}, text_output=text_output.getvalue(), node_type=self.node_type)
        except Exception as e:
            return NodeResult(success=False, error=str(e), text_output=text_output.getvalue(), status="failed", node_type=self.node_type)


class LogicNode(Node):
    """逻辑节点 - 生产数据"""
    
    def __init__(self, name: Optional[str] = None, logic_code: Optional[str] = None):
        if name is None:
            name = f"logic_{generate_random_name()}"

        super().__init__(name)
        
        self.logic_code = logic_code
    
    def set_logic(self, logic_code: str):
        """设置逻辑代码"""
        self.logic_code = logic_code
    
    def execute(self, context: Dict[str, Any], job_date: str = None, placeholders: Dict[str, Any] = None) -> NodeResult:
        """执行逻辑节点"""
        if not self.logic_code:
            return NodeResult(success=False, error="No logic code provided", node_type=self.node_type)
        
        # 合并来自上游节点的flow_context
        self.merge_flow_context_from_inputs()
        
        # 创建输出捕获
        text_output = io.StringIO()
        try:
            # 创建本地变量空间，包含输入数据、上下文和flow_context
            local_vars = {**self.inputs, **context, **self.get_flow_context_values()}
            # 添加pandas导入（但不包含在flow_context中）
            local_vars['pd'] = pd
            # 解析动态参数
            parsed_code = parse_dynamic_parameters(self.logic_code, job_date, placeholders)
            # 将解析后的代码中的动态参数替换到local_vars中
            for key, value in local_vars.items():
                if isinstance(value, str) and '${' in value:
                    local_vars[key] = parse_dynamic_parameters(value, job_date, placeholders)
            with redirect_stdout(text_output):
                exec(parsed_code, {}, local_vars)

            # 更新flow_context - 将输出数据中需要跟踪的变量添加到flow_context
            for var_name in self.tracked_variables:
                if var_name in local_vars:
                    self.update_flow_context(var_name, local_vars[var_name], context=context)
            # 返回所有tracked_variables的值作为输出数据
            output_data = self.get_flow_context_values()
            output_data = {var: output_data[var] for var in self.tracked_variables}            
            return NodeResult(success=True, data=output_data, text_output=text_output.getvalue(), node_type=self.node_type)
        except Exception as e:
            return NodeResult(success=False, error=str(e), text_output=text_output.getvalue(), status="failed", node_type=self.node_type)


class CollectionNode(Node):
    """Collection节点 - 收集多个上游数据"""
    
    def __init__(self, name: Optional[str] = None, logic_code: Optional[str] = None):
        if name is None:
            name = f"collection_{generate_random_name()}"

        super().__init__(name)
        self.logic_code = logic_code
        self.collected_data = {}
    
    def set_logic(self, logic_code: str):
        """设置逻辑代码"""
        self.logic_code = logic_code
    
    def execute(self, context: Dict[str, Any], job_date: str = None, placeholders: Dict[str, Any] = None) -> NodeResult:
        """执行collection节点"""
        try:
            # 合并来自上游节点的flow_context
            self.merge_flow_context_from_inputs()
            
            # 收集所有上游节点的数据
            collected_items = {}
            for input_name, input_data in self.inputs.items():
                # 跳过上下文数据
                if input_name in context:
                    continue
                
                # 如果input_data是节点对象，获取其flow_context
                if input_name.startswith("__node_") and hasattr(input_data, 'flow_context') and isinstance(input_data.flow_context, dict):
                    node_name = input_data.name
                    flow_context_values = input_data.get_flow_context_values()
                    if flow_context_values:
                        schema_values = {}
                        for var_name in flow_context_values:
                            if var_name in self.expected_input_schema:
                                schema_str = self.expected_input_schema[var_name]
                                validator = DataValidator.from_string(schema_str)
                                if validator.validate(flow_context_values[var_name]):
                                    schema_values[var_name] = flow_context_values[var_name]
                        
                        # 长度相等，才能往下游走，否则跳过
                        if len(schema_values) == len(self.expected_input_schema):
                            collected_items[node_name] = schema_values


            self.collected_data = collected_items
            # 如果有logic_code，执行逻辑处理
            if self.logic_code:
                # 创建输出捕获
                text_output = io.StringIO()
                try:
                    # 创建本地变量空间，包含收集的数据和上下文
                    local_vars = {**self.inputs, **context}
                    local_vars['collection'] = collected_items  # 添加collection变量
                    
                    # 解析动态参数
                    parsed_code = parse_dynamic_parameters(self.logic_code, job_date, placeholders)
                    
                    with redirect_stdout(text_output):
                        exec(parsed_code, local_vars, local_vars)
                    
                    # 更新flow_context - 将输出数据中需要跟踪的变量添加到flow_context
                    for var_name in self.tracked_variables:
                        if var_name in local_vars:
                            self.update_flow_context(var_name, local_vars[var_name])
                    
                    # 返回所有tracked_variables的值作为输出数据
                    output_data = self.get_flow_context_values()
                    output_data = {var: output_data[var] for var in self.tracked_variables}
                    return NodeResult(success=True, data=output_data, text_output=text_output.getvalue(), node_type=self.node_type)
                    
                except Exception as e:
                    return NodeResult(success=False, error=str(e), text_output=text_output.getvalue(), status="failed", node_type=self.node_type)
            else:
                # 没有logic_code，返回原始收集数据
                output_data = {'collected_data': collected_items}
                return NodeResult(success=True, data=output_data, node_type=self.node_type)
                
        except Exception as e:
            return NodeResult(success=False, error=str(e), status="failed", node_type=self.node_type)


def parse_dynamic_parameters(text: str, job_date: str = None, placeholders: Dict[str, Any] = None) -> str:
    """解析动态参数，支持时间参数和占位符"""
    if not text:
        return text
    
    result = text
    
    # 处理时间相关的占位符
    if job_date:
        try:
            # 将job_date转换为datetime对象作为基准时间，支持两种格式
            base_datetime = parse_job_date(job_date)
            result = parse_datetime(result, base_datetime)
        except Exception as e:
            # 如果解析失败，记录错误但不抛出异常
            print(f"Warning: Error parsing datetime parameter: {e}")
    
    # 处理普通占位符（如${store_nbr}、${item_nbr}等）
    if placeholders:
        for key, value in placeholders.items():
            placeholder = f"${{{key}}}"
            if placeholder in result:
                result = result.replace(placeholder, str(value))
    
    return result


def parse_job_date(job_date: str) -> datetime:
    """
    解析job_date，支持两种格式：
    - '%Y-%m-%d' (如: '2024-01-01')
    - '%Y-%m-%d %H:%M:%S' (如: '2024-01-01 10:30:00')
    
    Args:
        job_date: 日期字符串
        
    Returns:
        datetime: 解析后的datetime对象
        
    Raises:
        ValueError: 如果日期格式不支持
    """
    # 尝试解析为完整日期时间格式
    try:
        return datetime.strptime(job_date, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        pass
    
    # 尝试解析为仅日期格式
    try:
        return datetime.strptime(job_date, '%Y-%m-%d')
    except ValueError:
        pass
    
    # 如果两种格式都失败，抛出异常
    raise ValueError(f"不支持的日期格式: {job_date}。支持的格式: 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'")
