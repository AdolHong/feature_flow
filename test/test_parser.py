import pandas as pd
import sys
import os

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.parser import Parser, parse_excel_file, parse_excel_row


def test_parser():
    """测试解析器功能"""
    
    # 读取Excel文件
    df = pd.read_excel('./tmp.xlsx')
    print(f"原始数据形状: {df.shape}")
    print(f"列名: {df.columns.tolist()}")
    
    # 创建解析器
    parser = Parser()
    
    # 解析第一行数据
    print("\n=== 解析第一行数据 ===")
    first_row = df.iloc[0]
    parsed_row = parser.parse_row(first_row)
    
    for key, value in parsed_row.items():
        print(f"{key}: {type(value).__name__} = {value}")
        if isinstance(value, pd.DataFrame):
            print(f"  DataFrame形状: {value.shape}")
            print(f"  DataFrame列名: {value.columns.tolist()}")
            print(f"  前3行数据:")
            print(value.head(3))
        elif isinstance(value, dict):
            print(f"  字典内容: {value}")
    
    # 解析整个DataFrame
    print("\n=== 解析整个DataFrame ===")
    all_parsed = parser.parse_dataframe(df)
    print(f"解析了 {len(all_parsed)} 行数据")
    
    # 检查df_calendar字段的类型
    print("\n=== 检查df_calendar字段类型 ===")
    for i, row_data in enumerate(all_parsed):
        if 'df_calendar' in row_data:
            calendar_data = row_data['df_calendar']
            print(f"第 {i+1} 行 df_calendar: {type(calendar_data).__name__}")
            if isinstance(calendar_data, dict):
                print(f"  字典键: {list(calendar_data.keys())}")
            elif isinstance(calendar_data, pd.DataFrame):
                print(f"  DataFrame形状: {calendar_data.shape}")


def test_json_parsing():
    """测试JSON解析功能"""
    print("\n=== 测试JSON解析功能 ===")
    
    parser = Parser()
    
    # 测试数组长度一致的JSON
    consistent_json = '{"type":"dataframe","records":"{\\"col1\\":[1,2,3],\\"col2\\":[\\"a\\",\\"b\\",\\"c\\"]}"}'
    print(f"测试数组长度一致的JSON:")
    print(f"  输入: {consistent_json}")
    result = parser._parse_string_value(consistent_json, "test")
    print(f"  结果类型: {type(result).__name__}")
    if isinstance(result, pd.DataFrame):
        print(f"  DataFrame形状: {result.shape}")
        print(f"  DataFrame内容:\n{result}")
    
    # 测试数组长度不一致的JSON
    inconsistent_json = '{"type":"dataframe","records":"{\\"col1\\":[1,2],\\"col2\\":[\\"a\\",\\"b\\",\\"c\\"]}"}'
    print(f"\n测试数组长度不一致的JSON:")
    print(f"  输入: {inconsistent_json}")
    result = parser._parse_string_value(inconsistent_json, "test")
    print(f"  结果类型: {type(result).__name__}")
    if isinstance(result, dict):
        print(f"  字典内容: {result}")


if __name__ == "__main__":
    test_parser()
    test_json_parsing() 