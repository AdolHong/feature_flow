import pandas as pd
import sys
import os

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.parser import Parser, parse_dict


def test_dict_parser():
    """测试字典解析功能"""
    print("=== 测试字典解析功能 ===")
    
    # 创建解析器
    parser = Parser()
    
    # 测试数据 - 模拟Excel行数据
    test_dict = {
        'store_nbr': '5086',
        'item_nbr': '980065043',
        'job_date': '2025-08-04',
        'event_timestamp': '2025-08-04 08:00:00',
        'dim_store': '{"store_nbr":"5086","province_cn":"福建","province_en":"fujian","city_en":"fuzhou","city_cn":"福州","mother_store_id":"6507"}',
        'df_calendar': '{"type":"dataframe","records":"{\\"gregorian_date\\":[\\"2025-08-04\\",\\"2025-08-05\\"],\\"holiday_name\\":[\\"The Qi Xi Festival\\",\\"Teacher\\\'s Day\\"]}"}',
        'df_tft_forecat': '{"etl_load_time":"2025-08-04T13:17:33.910+08:00","type":"dataframe","value":"{\\"fcst_date\\":[\\"2025-08-04\\",\\"2025-08-05\\"],\\"fcst_qty\\":[0.0,0.0]}"}',
        'df_hist_sale': '{"type":"dataframe","records":"{\\"ts\\":[\\"2025-07-07\\",\\"2025-07-09\\"],\\"order_quantity\\":[0.0,1.0],\\"is_activate\\":[1,1]}"}',
        'integer_str': '123',
        'float_str': '123.45',
        'bool_str': 'true',
        'normal_str': 'hello world',
        'none_str': None,
    }
    
    print("原始字典数据:")
    for key, value in test_dict.items():
        print(f"  {key}: {value}")
    
    # 解析字典
    print("\n解析后的结果:")
    parsed_dict = parser.parse_dict(test_dict)
    
    for key, value in parsed_dict.items():
        print(f"  {key}: {type(value).__name__} = {value}")
        if isinstance(value, pd.DataFrame):
            print(f"    DataFrame形状: {value.shape}")
            print(f"    DataFrame列名: {value.columns.tolist()}")
            print(f"    DataFrame内容:\n{value}")
        elif isinstance(value, dict):
            print(f"    字典内容: {value}")


def test_consistent_vs_inconsistent_arrays():
    """测试数组长度一致和不一致的情况"""
    print("\n=== 测试数组长度一致和不一致的情况 ===")
    
    parser = Parser()
    
    # 数组长度一致的字典
    consistent_dict = {
        'consistent_df': '{"type":"dataframe","records":"{\\"col1\\":[1,2,3],\\"col2\\":[\\"a\\",\\"b\\",\\"c\\"]}"}'
    }
    
    # 数组长度不一致的字典
    inconsistent_dict = {
        'inconsistent_df': '{"type":"dataframe","records":"{\\"col1\\":[1,2],\\"col2\\":[\\"a\\",\\"b\\",\\"c\\"]}"}'
    }
    
    print("数组长度一致的字典:")
    parsed_consistent = parser.parse_dict(consistent_dict)
    for key, value in parsed_consistent.items():
        print(f"  {key}: {type(value).__name__}")
        if isinstance(value, pd.DataFrame):
            print(f"    DataFrame形状: {value.shape}")
            print(f"    DataFrame内容:\n{value}")
    
    print("\n数组长度不一致的字典:")
    parsed_inconsistent = parser.parse_dict(inconsistent_dict)
    for key, value in parsed_inconsistent.items():
        print(f"  {key}: {type(value).__name__}")
        if isinstance(value, dict):
            print(f"    字典内容: {value}")


def test_compare_with_excel_parser():
    """比较dict解析器和Excel解析器的结果"""
    print("\n=== 比较dict解析器和Excel解析器的结果 ===")
    
    parser = Parser()
    
    # 读取Excel文件的第一行
    df = pd.read_excel('./tmp.xlsx')
    first_row = df.iloc[0]
    
    # 将Series转换为dict
    row_dict = first_row.to_dict()
    
    print("使用Excel解析器解析Series:")
    parsed_series = parser.parse_row(first_row)
    
    print("使用dict解析器解析字典:")
    parsed_dict = parser.parse_dict(row_dict)
    
    # 比较结果
    print("\n比较结果:")
    for key in parsed_series.keys():
        if key in parsed_dict:
            series_type = type(parsed_series[key]).__name__
            dict_type = type(parsed_dict[key]).__name__
            print(f"  {key}: Series解析={series_type}, Dict解析={dict_type}")
            if series_type != dict_type:
                print(f"    类型不一致！")


def test_convenience_function():
    """测试便捷函数"""
    print("\n=== 测试便捷函数 ===")
    
    test_dict = {
        'integer_str': '123',
        'json_str': '{"name": "test", "value": 42}',
        'dataframe_json': '{"type":"dataframe","records":"{\\"col1\\":[1,2,3],\\"col2\\":[\\"a\\",\\"b\\",\\"c\\"]}"}'
    }
    
    print("使用便捷函数parse_dict:")
    parsed = parse_dict(test_dict)
    
    for key, value in parsed.items():
        print(f"  {key}: {type(value).__name__} = {value}")


if __name__ == "__main__":
    test_dict_parser()
    test_consistent_vs_inconsistent_arrays()
    test_compare_with_excel_parser()
    test_convenience_function() 