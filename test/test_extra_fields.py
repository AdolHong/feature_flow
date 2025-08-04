import pandas as pd
import sys
import os

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.parser import Parser, parse_dict


def test_extra_fields_as_columns():
    """测试额外字段作为DataFrame列的功能"""
    print("=== 测试额外字段作为DataFrame列的功能 ===")
    
    parser = Parser()
    
    # 测试用例1: 包含额外字段的JSON
    test_dict_1 = {
        'df_with_extra': '{"type":"dataframe","records":"{\\"col1\\":[1,2,3],\\"col2\\":[\\"a\\",\\"b\\",\\"c\\"]}","etl_load_time":"2025-08-04T13:17:33.910+08:00","source":"api","version":"1.0"}'
    }
    
    # 测试用例2: 不包含额外字段的JSON
    test_dict_2 = {
        'df_no_extra': '{"type":"dataframe","records":"{\\"col1\\":[1,2,3],\\"col2\\":[\\"a\\",\\"b\\",\\"c\\"]}"}'
    }
    
    # 测试用例3: 包含多个额外字段的JSON
    test_dict_3 = {
        'df_multiple_extra': '{"type":"dataframe","records":"{\\"ts\\":[\\"2025-07-07\\",\\"2025-07-09\\"],\\"value\\":[0.0,1.0]}","etl_load_time":"2025-08-04T13:17:33.910+08:00","source":"database","version":"2.1","status":"active","metadata":"{\\"description\\":\\"test data\\"}"}'
    }
    
    print("测试用例1: 包含额外字段的JSON")
    parsed_1 = parser.parse_dict(test_dict_1)
    for key, value in parsed_1.items():
        print(f"  {key}: {type(value).__name__}")
        if isinstance(value, pd.DataFrame):
            print(f"    DataFrame形状: {value.shape}")
            print(f"    DataFrame列名: {value.columns.tolist()}")
            print(f"    DataFrame内容:\n{value}")
    
    print("\n测试用例2: 不包含额外字段的JSON")
    parsed_2 = parser.parse_dict(test_dict_2)
    for key, value in parsed_2.items():
        print(f"  {key}: {type(value).__name__}")
        if isinstance(value, pd.DataFrame):
            print(f"    DataFrame形状: {value.shape}")
            print(f"    DataFrame列名: {value.columns.tolist()}")
            print(f"    DataFrame内容:\n{value}")
    
    print("\n测试用例3: 包含多个额外字段的JSON")
    parsed_3 = parser.parse_dict(test_dict_3)
    for key, value in parsed_3.items():
        print(f"  {key}: {type(value).__name__}")
        if isinstance(value, pd.DataFrame):
            print(f"    DataFrame形状: {value.shape}")
            print(f"    DataFrame列名: {value.columns.tolist()}")
            print(f"    DataFrame内容:\n{value}")


def test_real_data_with_extra_fields():
    """测试真实数据中的额外字段"""
    print("\n=== 测试真实数据中的额外字段 ===")
    
    parser = Parser()
    
    # 模拟df_tft_forecat的真实数据格式
    real_data = {
        'df_tft_forecat': '{"etl_load_time":"2025-08-04T13:17:33.910+08:00","type":"dataframe","value":"{\\"fcst_date\\":[\\"2025-08-04\\",\\"2025-08-05\\"],\\"fcst_qty\\":[0.0,0.0]}"}'
    }
    
    print("解析df_tft_forecat数据:")
    parsed = parser.parse_dict(real_data)
    for key, value in parsed.items():
        print(f"  {key}: {type(value).__name__}")
        if isinstance(value, pd.DataFrame):
            print(f"    DataFrame形状: {value.shape}")
            print(f"    DataFrame列名: {value.columns.tolist()}")
            print(f"    DataFrame内容:\n{value}")
        elif isinstance(value, dict):
            print(f"    字典内容: {value}")


def test_edge_cases():
    """测试边界情况"""
    print("\n=== 测试边界情况 ===")
    
    parser = Parser()
    
    # 测试用例1: 额外字段值为None
    test_dict_1 = {
        'df_none_extra': '{"type":"dataframe","records":"{\\"col1\\":[1,2],\\"col2\\":[\\"a\\",\\"b\\"]}","extra_field":null}'
    }
    
    # 测试用例2: 额外字段值为空字符串
    test_dict_2 = {
        'df_empty_extra': '{"type":"dataframe","records":"{\\"col1\\":[1,2],\\"col2\\":[\\"a\\",\\"b\\"]}","extra_field":""}'
    }
    
    # 测试用例3: 额外字段值为数字
    test_dict_3 = {
        'df_number_extra': '{"type":"dataframe","records":"{\\"col1\\":[1,2],\\"col2\\":[\\"a\\",\\"b\\"]}","extra_field":42}'
    }
    
    print("测试用例1: 额外字段值为None")
    parsed_1 = parser.parse_dict(test_dict_1)
    for key, value in parsed_1.items():
        print(f"  {key}: {type(value).__name__}")
        if isinstance(value, pd.DataFrame):
            print(f"    DataFrame形状: {value.shape}")
            print(f"    DataFrame列名: {value.columns.tolist()}")
            print(f"    DataFrame内容:\n{value}")
    
    print("\n测试用例2: 额外字段值为空字符串")
    parsed_2 = parser.parse_dict(test_dict_2)
    for key, value in parsed_2.items():
        print(f"  {key}: {type(value).__name__}")
        if isinstance(value, pd.DataFrame):
            print(f"    DataFrame形状: {value.shape}")
            print(f"    DataFrame列名: {value.columns.tolist()}")
            print(f"    DataFrame内容:\n{value}")
    
    print("\n测试用例3: 额外字段值为数字")
    parsed_3 = parser.parse_dict(test_dict_3)
    for key, value in parsed_3.items():
        print(f"  {key}: {type(value).__name__}")
        if isinstance(value, pd.DataFrame):
            print(f"    DataFrame形状: {value.shape}")
            print(f"    DataFrame列名: {value.columns.tolist()}")
            print(f"    DataFrame内容:\n{value}")


if __name__ == "__main__":
    test_extra_fields_as_columns()
    test_real_data_with_extra_fields()
    test_edge_cases() 