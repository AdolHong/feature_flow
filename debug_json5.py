#!/usr/bin/env python3
"""
调试JSON5解析问题
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json5
from core.engine import RuleEngine

def test_json5_parsing():
    """测试JSON5解析"""
    
    # 修复后的JSON5字符串
    encoded_engine = """
{
  name: "sams_demand_forecast_engine",
  nodes: {
    preprocess: {
      type: "LogicNode",
      name: "preprocess",
      tracked_variables: [
        "city_cn",
        "category_nbr",
        "is_fresh",
      ],
      expected_input_schema: {},
      logic_code: "\n# 提取城市和分类\ncity_cn = dim_store.get('city_cn', '未知城市')\ncategory_nbr = int(dim_item.get('category_nbr', 0))\nis_fresh = dim_item.get('is_fresh', 0)\n",
    },
    base_forecast: {
      type: "LogicNode",
      name: "base_forecast",
      tracked_variables: [
        "output",
      ],
      expected_input_schema: {},
      logic_code: "\nxgb_forecast['priority'] = 80\nxgb_forecast['model'] = 'xgb'\noutput = xgb_forecast\n",
    },
    tft_forecast: {
      type: "LogicNode",
      name: "tft_forecast",
      tracked_variables: [
        "output",
      ],
      expected_input_schema: {},
      logic_code: "\ntft_forecast['priority'] = 90\ntft_forecast['model'] = 'tft'\noutput = tft_forecast\n",
    },
    fence_forecast: {
      type: "LogicNode",
      name: "fence_forecast",
      tracked_variables: [
        "output",
      ],
      expected_input_schema: {},
      logic_code: "\nfence_forecast['priority'] = 100\nfence_forecast['model'] = 'xgb_fence'\noutput = fence_forecast\n",
    },
    prophet_forecast: {
      type: "LogicNode",
      name: "prophet_forecast",
      tracked_variables: [
        "output",
      ],
      expected_input_schema: {},
      logic_code: "\nprophet_forecast['priority'] = 10\nprophet_forecast['model'] = 'prophet_ma_3'\noutput = prophet_forecast\n",
    },
    coll: {
      type: "CollectionNode",
      name: "coll",
      tracked_variables: [
        "output",
        "df_concat",
      ],
      expected_input_schema: {
        "output": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string,priority:int,model:string",
      },
      logic_code: "\nimport pandas as pd     \n# 拼接上游的输出; 不关心node_name      \ndf_concat = pd.concat([collection[node_name]['output'] for node_name in collection])\ndf_concat.sort_values(by=[\"fcst_date\", \"model\"], inplace=True) # 按fcst_date和model排序\n\n# 按fcst_date分组，取priority最高的行，并计算平均值\noutput = df_concat.groupby(\"fcst_date\").apply(\n    lambda group: group[group[\"priority\"] == group[\"priority\"].max()].mean(numeric_only=True)\n).reset_index()\n                        \n# 补充非数值列，并将model拼接为字符串\nfor fcst_date in output[\"fcst_date\"]:\n    original_group = df_concat[df_concat['fcst_date'] == fcst_date]\n    max_priority_group = original_group[original_group[\"priority\"] == original_group[\"priority\"].max()]\n    \n    # 获取最高priority的所有model，用逗号拼接\n    models = max_priority_group[\"model\"].tolist()\n    model_str = \",\".join(models)\n    \n    # 取第一行的其他非数值列\n    first_row = max_priority_group.iloc[0]\n    output.loc[output[\"fcst_date\"] == fcst_date, \"model\"] = model_str\noutput['store_nbr'] = store_nbr\noutput['item_nbr'] = item_nbr\n",
    },
  },
  dependencies: {
    start_node: [],
    preprocess: [
      "start_node",
    ],
    base_forecast: [
      "start_node",
    ],
    tft_forecast: [
      "start_node",
    ],
    fence_forecast: [
      "start_node",
    ],
    prophet_forecast: [
      "start_node",
    ],
    coll: [
      "base_forecast",
      "tft_forecast",
      "fence_forecast",
      "prophet_forecast",
    ],
  },
}
"""
    
    print("=== 测试JSON5解析 ===")
    
    try:
        # 尝试解析JSON5
        config = json5.loads(encoded_engine)
        print("✅ JSON5解析成功!")
        print(f"引擎名称: {config['name']}")
        print(f"节点数量: {len(config['nodes'])}")
        print(f"依赖关系数量: {len(config['dependencies'])}")
        
        # 尝试创建引擎
        engine = RuleEngine.import_from_json(encoded_engine)
        print("✅ 引擎创建成功!")
        print(f"引擎名称: {engine.name}")
        print(f"节点数量: {len(engine.get_all_nodes())}")
        
        return engine
        
    except Exception as e:
        print(f"❌ JSON5解析失败: {e}")
        print(f"错误类型: {type(e)}")
        
        # 尝试修复常见问题
        print("\n=== 尝试修复JSON5格式 ===")
        
        # 1. 检查字符串中的转义字符
        fixed_engine = encoded_engine.replace('\\n', '\n')
        print("1. 修复换行符...")
        
        try:
            config = json5.loads(fixed_engine)
            print("✅ 修复换行符后解析成功!")
            return RuleEngine.import_from_json(fixed_engine)
        except Exception as e2:
            print(f"❌ 修复换行符后仍然失败: {e2}")
        
        # 2. 尝试使用标准JSON格式
        print("2. 尝试转换为标准JSON格式...")
        try:
            # 将无引号键名添加引号
            import re
            json_str = re.sub(r'(\w+):', r'"\1":', fixed_engine)
            config = json5.loads(json_str)
            print("✅ 转换为标准JSON后解析成功!")
            return RuleEngine.import_from_json(json_str)
        except Exception as e3:
            print(f"❌ 转换为标准JSON后仍然失败: {e3}")
        
        return None

def test_engine_execution(engine):
    """测试引擎执行"""
    if not engine:
        print("引擎未创建，跳过执行测试")
        return
    
    print("\n=== 测试引擎执行 ===")
    
    try:
        # 模拟执行参数
        job_date = "2024-01-15"
        placeholders = {
            "store_nbr": "001",
            "item_nbr": "12345",
            "dim_store": {"city_cn": "北京"},
            "dim_item": {"category_nbr": "1", "is_fresh": "0"},
            "xgb_forecast": {"fcst_date": "2024-01-16", "fcst_qty": 100},
            "tft_forecast": {"fcst_date": "2024-01-16", "fcst_qty": 110},
            "fence_forecast": {"fcst_date": "2024-01-16", "fcst_qty": 105},
            "prophet_forecast": {"fcst_date": "2024-01-16", "fcst_qty": 95}
        }
        
        # 执行引擎
        results = engine.execute(
            job_date=job_date,
            placeholders=placeholders
        )
        
        print("✅ 引擎执行成功!")
        
        # 显示执行结果
        for node_name, result in results.items():
            if node_name == "start_node":
                continue
            print(f"  {node_name}: {'✅' if result.success else '❌'}")
            if not result.success:
                print(f"    错误: {result.error}")
        
    except Exception as e:
        print(f"❌ 引擎执行失败: {e}")
        import traceback
        traceback.print_exc()

def create_fixed_engine_string():
    """创建修复后的引擎字符串"""
    return """
{
  "name": "sams_demand_forecast_engine",
  "nodes": {
    "preprocess": {
      "type": "LogicNode",
      "name": "preprocess",
      "tracked_variables": [
        "city_cn",
        "category_nbr",
        "is_fresh"
      ],
      "expected_input_schema": {},
      "logic_code": "\n# 提取城市和分类\ncity_cn = dim_store.get('city_cn', '未知城市')\ncategory_nbr = int(dim_item.get('category_nbr', 0))\nis_fresh = dim_item.get('is_fresh', 0)\n"
    },
    "base_forecast": {
      "type": "LogicNode",
      "name": "base_forecast",
      "tracked_variables": [
        "output"
      ],
      "expected_input_schema": {},
      "logic_code": "\nxgb_forecast['priority'] = 80\nxgb_forecast['model'] = 'xgb'\noutput = xgb_forecast\n"
    },
    "tft_forecast": {
      "type": "LogicNode",
      "name": "tft_forecast",
      "tracked_variables": [
        "output"
      ],
      "expected_input_schema": {},
      "logic_code": "\ntft_forecast['priority'] = 90\ntft_forecast['model'] = 'tft'\noutput = tft_forecast\n"
    },
    "fence_forecast": {
      "type": "LogicNode",
      "name": "fence_forecast",
      "tracked_variables": [
        "output"
      ],
      "expected_input_schema": {},
      "logic_code": "\nfence_forecast['priority'] = 100\nfence_forecast['model'] = 'xgb_fence'\noutput = fence_forecast\n"
    },
    "prophet_forecast": {
      "type": "LogicNode",
      "name": "prophet_forecast",
      "tracked_variables": [
        "output"
      ],
      "expected_input_schema": {},
      "logic_code": "\nprophet_forecast['priority'] = 10\nprophet_forecast['model'] = 'prophet_ma_3'\noutput = prophet_forecast\n"
    },
    "coll": {
      "type": "CollectionNode",
      "name": "coll",
      "tracked_variables": [
        "output",
        "df_concat"
      ],
      "expected_input_schema": {
        "output": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string,priority:int,model:string"
      },
      "logic_code": "\nimport pandas as pd     \n# 拼接上游的输出; 不关心node_name      \ndf_concat = pd.concat([collection[node_name]['output'] for node_name in collection])\ndf_concat.sort_values(by=[\"fcst_date\", \"model\"], inplace=True) # 按fcst_date和model排序\n\n# 按fcst_date分组，取priority最高的行，并计算平均值\noutput = df_concat.groupby(\"fcst_date\").apply(\n    lambda group: group[group[\"priority\"] == group[\"priority\"].max()].mean(numeric_only=True)\n).reset_index()\n                        \n# 补充非数值列，并将model拼接为字符串\nfor fcst_date in output[\"fcst_date\"]:\n    original_group = df_concat[df_concat['fcst_date'] == fcst_date]\n    max_priority_group = original_group[original_group[\"priority\"] == original_group[\"priority\"].max()]\n    \n    # 获取最高priority的所有model，用逗号拼接\n    models = max_priority_group[\"model\"].tolist()\n    model_str = \",\".join(models)\n    \n    # 取第一行的其他非数值列\n    first_row = max_priority_group.iloc[0]\n    output.loc[output[\"fcst_date\"] == fcst_date, \"model\"] = model_str\noutput['store_nbr'] = store_nbr\noutput['item_nbr'] = item_nbr\n"
    }
  },
  "dependencies": {
    "start_node": [],
    "preprocess": [
      "start_node"
    ],
    "base_forecast": [
      "start_node"
    ],
    "tft_forecast": [
      "start_node"
    ],
    "fence_forecast": [
      "start_node"
    ],
    "prophet_forecast": [
      "start_node"
    ],
    "coll": [
      "base_forecast",
      "tft_forecast",
      "fence_forecast",
      "prophet_forecast"
    ]
  }
}
"""

def test_standard_json():
    """测试标准JSON格式"""
    print("\n=== 测试标准JSON格式 ===")
    
    try:
        fixed_engine = create_fixed_engine_string()
        engine = RuleEngine.import_from_json(fixed_engine)
        print("✅ 标准JSON格式解析成功!")
        print(f"引擎名称: {engine.name}")
        print(f"节点数量: {len(engine.get_all_nodes())}")
        return engine
    except Exception as e:
        print(f"❌ 标准JSON格式解析失败: {e}")
        return None

if __name__ == "__main__":
    # 首先尝试JSON5格式
    engine = test_json5_parsing()
    
    # 如果JSON5失败，尝试标准JSON格式
    if not engine:
        engine = test_standard_json()
    
    # 测试引擎执行
    test_engine_execution(engine) 