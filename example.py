#!/usr/bin/env python3
"""
示例程序：从Redis加载数据并使用规则引擎处理
"""
import json
import pandas as pd

from data.redis_connector import RedisConnector
from data.dataloader import DataLoader
from core.engine import RuleEngine
from core.nodes import LogicNode, GateNode, CollectionNode


def create_data_config():
    """创建数据配置"""
    return {
        "namespace": "sams_cloud_demand_fcst_tn",
        "placeholder": [
            {"name": "store_nbr", "alias": None, "description": "门店号"},
            {"name": "item_nbr", "alias": None, "description": "商品号"},
        ],
        "variable": [
            # 商品维度信息
            {
                "name": "dim_item",
                "alias": None,
                "description": "商品维度信息",
                "namespace": "sams",
                "redis_config": {
                    "prefix": [
                        {"key": "item_nbr", "value": "${item_nbr}"}
                    ],
                    "field": "dim_item",
                    "type": "json"
                }
            },
            # 门店维度信息
            {
                "name": "dim_store",
                "alias": None,
                "description": "门店维度信息",
                "namespace": "sams_cloud",
                "redis_config": {
                    "prefix": [
                        {"key": "store_nbr", "value": "${store_nbr}"}
                    ],
                    "field": "dim_store",
                    "type": "json"
                }
            },
            # XGB预测数据
            {
                "name": "xgb_forecast",
                "alias": None,
                "description": "XGB预测数据",
                "namespace": None,
                "redis_config": {
                    "prefix": [
                        {"key": "store_nbr", "value": "${store_nbr}"},
                        {"key": "item_nbr", "value": "${item_nbr}"}
                    ],
                    "field": "xgb_restore__lgb_0_2024-08-13",
                    "type": "timeseries",
                    "from_datetime": "${yyyy-MM-dd}",
                    "to_datetime": "${yyyy-MM-dd+6d}",
                }
            },
            # TFT预测数据
            {
                "name": "tft_forecast",
                "alias": None,
                "description": "TFT预测数据",
                "namespace": None,
                "redis_config": {
                    "prefix": [
                        {"key": "store_nbr", "value": "${store_nbr}"},
                        {"key": "item_nbr", "value": "${item_nbr}"},
                    ],
                    "field": "tft_restore_20240926",
                    "type": "timeseries",
                    "from_datetime": "${yyyy-MM-dd}",
                    "to_datetime": "${yyyy-MM-dd+6d}",
                }
            }
        ]
    }


def create_rule_engine():
    """创建规则引擎"""
    engine = RuleEngine("sams_demand_forecast_engine")
    
    # 创建逻辑节点1：数据预处理
    preprocess_node = LogicNode("preprocess")
    preprocess_node.set_logic("""
# 提取城市和分类
city_cn = dim_store.get('city_cn', '未知城市')
category_nbr = int(dim_item.get('category_nbr', 0))
is_fresh = dim_item.get('is_fresh', 0)
""")
    preprocess_node.set_tracked_variables(["city_cn", "category_nbr", "is_fresh"])
    
    gate_cate66_node = GateNode("gate_cate66")
    gate_cate66_node.set_condition("category_nbr == 66")
    
    # 创建逻辑节点2：预测分析
    tft_forecast_node = LogicNode("tft_forecast")
    tft_forecast_node.set_logic("""
tft_forecast['priority'] = 90
tft_forecast['model'] = 'tft'
output = tft_forecast
""")
    tft_forecast_node.set_tracked_variables(["output"])

    base_forecast_node = LogicNode("base_forecast")
    base_forecast_node.set_logic("""
xgb_forecast['priority'] = 80
xgb_forecast['model'] = 'xgb'
output = xgb_forecast
""")
    base_forecast_node.set_tracked_variables(["output"])

    coll_node = CollectionNode("coll")
    coll_node.add_expected_input_schema("output", "job_date:string,fcst_date:string,fcst_qty:double,etl_time:string,priority:int,model:string")
    coll_node.set_logic(
"""
import pandas as pd     
# 拼接上游的输出; 不关心node_name      
df_concat = pd.concat([collection[node_name]['output'] for node_name in collection])
df_concat.sort_values(by=["fcst_date", "model"], inplace=True) # 按fcst_date和model排序

# 按fcst_date分组，取priority最高的行，并计算平均值
output = df_concat.groupby("fcst_date").apply(
    lambda group: group[group["priority"] == group["priority"].max()].mean(numeric_only=True)
).reset_index()
                        
# 补充非数值列，并将model拼接为字符串
for fcst_date in output["fcst_date"]:
    original_group = df_concat[df_concat['fcst_date'] == fcst_date]
    max_priority_group = original_group[original_group["priority"] == original_group["priority"].max()]
    
    # 获取最高priority的所有model，用逗号拼接
    models = max_priority_group["model"].tolist()
    model_str = ",".join(models)
    
    # 取第一行的其他非数值列
    first_row = max_priority_group.iloc[0]
    output.loc[output["fcst_date"] == fcst_date, "model"] = model_str

output['job_date'] = "${yyyy-MM-dd}"
output['store_nbr'] = store_nbr
output['item_nbr'] = item_nbr
""")
    coll_node.set_tracked_variables(["output", "df_concat"])

    
    # 设置依赖关系
    engine.add_dependency(None, preprocess_node)
    engine.add_dependency(preprocess_node, base_forecast_node)
    engine.add_dependency(preprocess_node, gate_cate66_node)
    engine.add_dependency(gate_cate66_node, tft_forecast_node)
    engine.add_dependency(base_forecast_node, coll_node)
    engine.add_dependency(tft_forecast_node, coll_node)
    
    return engine


def main():
    """主函数"""
    print("🚀 开始执行山姆云仓需求预测分析示例")
    
    # 参数设置
    store_nbr = "6111"
    item_nbr = "981049286"
    job_datetime = "2025-07-22 01:00:00"
    
    # 获取当前日期和时间
    # import datetime
    # current_time = datetime.datetime.now()
    # formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S") # 格式化为 "YYYY-MM-DD HH:MM:SS" 格式的字符串
    # print(formatted_time)


    
    print(f"📊 分析参数:")
    print(f"  门店号: {store_nbr}")
    print(f"  商品号: {item_nbr}")
    print(f"  作业时间: {job_datetime}")
    
    try:
        # 1. 初始化Redis连接器
        print("\n🔌 初始化Redis连接...")
        connector = RedisConnector()
        
        # 2. 创建数据加载器
        print("📥 创建数据加载器...")
        loader = DataLoader(connector)
        
        # 3. 创建数据配置
        print("⚙️ 创建数据配置...")
        data_config = create_data_config()
        
        # 4. 加载数据
        print("📊 从Redis加载数据...")
        placeholders = {
            "store_nbr": store_nbr,
            "item_nbr": item_nbr
        }
        
        loaded_data = loader.load_data(data_config, placeholders, job_datetime)
        
        print("✅ 数据加载完成:")
        for var_name, var_data in loaded_data.items():
            if isinstance(var_data, pd.DataFrame):
                print(f"  {var_name}: DataFrame({len(var_data)}行)")
            else:
                print(f"  {var_name}: {type(var_data).__name__}")
        
        # 5. 创建规则引擎
        print("\n⚙️ 创建规则引擎...")
        engine = create_rule_engine()
        
        # 6. 执行规则引擎
        print("🔄 执行规则引擎...")
        results = engine.execute(job_datetime, placeholders=placeholders, variables=loaded_data)
        
        # 8. 输出结果
        print("\n📋 执行结果:")
        for node_name, result in results.items():
            if result.success:
                print(f"  ✅ {node_name}: 执行成功")
            else:
                print(f"  ❌ {node_name}: 执行失败 - {result.error}")
        
        # 9. 获取最终结果
        print("\n🎯 最终分析结果:")
        final_context = engine.get_node_flow_context("result_summary")
        if "final_report" in final_context:
            final_report = final_context["final_report"][0]
            print(json.dumps(final_report, ensure_ascii=False, indent=2))
        
        # 10. 输出执行摘要
        print("\n📊 执行摘要:")
        summary = engine.get_execution_summary()
        print(f"  总节点数: {summary['total_nodes']}")
        print(f"  成功节点: {summary['successful_nodes']}")
        print(f"  失败节点: {summary['failed_nodes']}")
        print(f"  阻塞节点: {summary['blocked_nodes']}")
        print(f"  成功率: {summary['success_rate']:.2%}")
        
        print("\n✅ 示例程序执行完成!")
        
    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 