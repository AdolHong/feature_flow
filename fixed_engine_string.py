#!/usr/bin/env python3
"""
修复后的引擎字符串
主要修复了JSON5语法错误：
1. expected_input_schema中的无引号键名"output"添加了引号
2. 移除了尾随逗号
3. 提供了标准JSON格式作为备选
"""

# 修复后的JSON5格式引擎字符串
FIXED_JSON5_ENGINE = """
{
  name: "sams_demand_forecast_engine",
  nodes: {
    preprocess: {
      type: "LogicNode",
      name: "preprocess",
      tracked_variables: [
        "city_cn",
        "category_nbr",
        "is_fresh"
      ],
      expected_input_schema: {},
      logic_code: "\n# 提取城市和分类\ncity_cn = dim_store.get('city_cn', '未知城市')\ncategory_nbr = int(dim_item.get('category_nbr', 0))\nis_fresh = dim_item.get('is_fresh', 0)\n"
    },
    base_forecast: {
      type: "LogicNode",
      name: "base_forecast",
      tracked_variables: [
        "output"
      ],
      expected_input_schema: {},
      logic_code: "\nxgb_forecast['priority'] = 80\nxgb_forecast['model'] = 'xgb'\noutput = xgb_forecast\n"
    },
    tft_forecast: {
      type: "LogicNode",
      name: "tft_forecast",
      tracked_variables: [
        "output"
      ],
      expected_input_schema: {},
      logic_code: "\ntft_forecast['priority'] = 90\ntft_forecast['model'] = 'tft'\noutput = tft_forecast\n"
    },
    fence_forecast: {
      type: "LogicNode",
      name: "fence_forecast",
      tracked_variables: [
        "output"
      ],
      expected_input_schema: {},
      logic_code: "\nfence_forecast['priority'] = 100\nfence_forecast['model'] = 'xgb_fence'\noutput = fence_forecast\n"
    },
    prophet_forecast: {
      type: "LogicNode",
      name: "prophet_forecast",
      tracked_variables: [
        "output"
      ],
      expected_input_schema: {},
      logic_code: "\nprophet_forecast['priority'] = 10\nprophet_forecast['model'] = 'prophet_ma_3'\noutput = prophet_forecast\n"
    },
    coll: {
      type: "CollectionNode",
      name: "coll",
      tracked_variables: [
        "output",
        "df_concat"
      ],
      expected_input_schema: {
        "output": "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string,priority:int,model:string"
      },
      logic_code: "\nimport pandas as pd     \n# 拼接上游的输出; 不关心node_name      \ndf_concat = pd.concat([collection[node_name]['output'] for node_name in collection])\ndf_concat.sort_values(by=[\"fcst_date\", \"model\"], inplace=True) # 按fcst_date和model排序\n\n# 按fcst_date分组，取priority最高的行，并计算平均值\noutput = df_concat.groupby(\"fcst_date\").apply(\n    lambda group: group[group[\"priority\"] == group[\"priority\"].max()].mean(numeric_only=True)\n).reset_index()\n                        \n# 补充非数值列，并将model拼接为字符串\nfor fcst_date in output[\"fcst_date\"]:\n    original_group = df_concat[df_concat['fcst_date'] == fcst_date]\n    max_priority_group = original_group[original_group[\"priority\"] == original_group[\"priority\"].max()]\n    \n    # 获取最高priority的所有model，用逗号拼接\n    models = max_priority_group[\"model\"].tolist()\n    model_str = \",\".join(models)\n    \n    # 取第一行的其他非数值列\n    first_row = max_priority_group.iloc[0]\n    output.loc[output[\"fcst_date\"] == fcst_date, \"model\"] = model_str\noutput['store_nbr'] = store_nbr\noutput['item_nbr'] = item_nbr\n"
    }
  },
  dependencies: {
    start_node: [],
    preprocess: [
      "start_node"
    ],
    base_forecast: [
      "start_node"
    ],
    tft_forecast: [
      "start_node"
    ],
    fence_forecast: [
      "start_node"
    ],
    prophet_forecast: [
      "start_node"
    ],
    coll: [
      "base_forecast",
      "tft_forecast",
      "fence_forecast",
      "prophet_forecast"
    ]
  }
}
"""

# 标准JSON格式引擎字符串（作为备选）
STANDARD_JSON_ENGINE = """
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

def get_engine_string(use_json5=True):
    """
    获取引擎字符串
    
    Args:
        use_json5 (bool): 是否使用JSON5格式，如果为False则使用标准JSON格式
    
    Returns:
        str: 引擎配置字符串
    """
    if use_json5:
        return FIXED_JSON5_ENGINE
    else:
        return STANDARD_JSON_ENGINE

def test_engine_string():
    """测试引擎字符串"""
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    try:
        from core.engine import RuleEngine
        import json5
        
        print("=== 测试修复后的引擎字符串 ===")
        
        # 测试JSON5格式
        print("1. 测试JSON5格式...")
        try:
            engine = RuleEngine.import_from_json(FIXED_JSON5_ENGINE)
            print("✅ JSON5格式解析成功!")
            print(f"引擎名称: {engine.name}")
            print(f"节点数量: {len(engine.get_all_nodes())}")
        except Exception as e:
            print(f"❌ JSON5格式解析失败: {e}")
        
        # 测试标准JSON格式
        print("\n2. 测试标准JSON格式...")
        try:
            engine = RuleEngine.import_from_json(STANDARD_JSON_ENGINE)
            print("✅ 标准JSON格式解析成功!")
            print(f"引擎名称: {engine.name}")
            print(f"节点数量: {len(engine.get_all_nodes())}")
        except Exception as e:
            print(f"❌ 标准JSON格式解析失败: {e}")
            
    except ImportError as e:
        print(f"导入错误: {e}")
        print("请确保已安装json5依赖: pip install json5")

if __name__ == "__main__":
    test_engine_string() 