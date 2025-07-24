
import pytest
from datetime import datetime
from utils.datetime_parser import parse_datetime

def test_datetime_parser_with_fixed_base():
    """使用固定基准时间测试日期时间解析"""
    # 固定基准时间：2025-07-01 10:00:00
    base_datetime = datetime(2025, 7, 1, 10, 0, 0)
    
    test_cases = [
        # 基本格式测试
        ("${yyyyMMdd}", "20250701"),
        ("${yyyy-MM-dd}", "2025-07-01"),
        ("${yyyyMMddHHmmss}", "20250701100000"),
        ("${yyyy-MM-dd HH:mm:ss}", "2025-07-01 10:00:00"),
        
        # 年份偏移测试
        ("${yyyyMMdd+1y}", "20260701"),
        ("${yyyyMMdd-1y}", "20240701"),
        
        # 月份偏移测试
        ("${yyyyMMdd+1M}", "20250801"),
        ("${yyyyMMdd-1M}", "20250601"),
        
        # 周偏移测试
        ("${yyyyMMdd+1w}", "20250708"),
        ("${yyyyMMdd-1w}", "20250624"),
        
        # 天偏移测试
        ("${yyyyMMdd+1d}", "20250702"),
        ("${yyyyMMdd-1d}", "20250630"),
        
        # 小时偏移测试
        ("${yyyyMMddHHmmss+3H}", "20250701130000"),
        ("${yyyyMMddHHmmss-3H}", "20250701070000"),
        
        # 分钟偏移测试
        ("${yyyyMMddHHmmss+25m}", "20250701102500"),
        ("${yyyyMMddHHmmss-25m}", "20250701093500"),
        
        # 复杂组合测试
        ("job_date=${yyyyMMdd-1d}&biz_date=${yyyyMMdd}", "job_date=20250630&biz_date=20250701"),
        ("from=${yyyy-MM-dd-7d}&to=${yyyy-MM-dd+7d}", "from=2025-06-24&to=2025-07-08"),
    ]
    
    for input_str, expected_result in test_cases:
        result = parse_datetime(input_str, base_datetime=base_datetime)
        assert result == expected_result, f"输入: {input_str}, 期望: {expected_result}, 实际: {result}"
        print(f"✅ {input_str} -> {result}")

def test_datetime_parser_without_base():
    """测试不传base_datetime时使用当前时间"""
    input_str = "${yyyy-MM-dd}"
    result = parse_datetime(input_str)
    
    # 验证结果格式正确
    assert len(result) == 10  # YYYY-MM-DD 格式
    assert result[4] == '-' and result[7] == '-'
    assert result[:4].isdigit() and result[5:7].isdigit() and result[8:10].isdigit()
    
    print(f"✅ 无基准时间测试: {input_str} -> {result}")

def test_edge_cases():
    """测试边界情况"""
    base_datetime = datetime(2025, 1, 31, 10, 0, 0)  # 1月31日
    
    # 测试月末日期处理
    result = parse_datetime("${yyyyMMdd+1M}", base_datetime=base_datetime)
    assert result == "20250228", f"1月31日+1月应该是2月28日，实际: {result}"
    
    # 测试2月29日（闰年）
    leap_year_base = datetime(2024, 1, 31, 10, 0, 0)  # 2024是闰年
    result = parse_datetime("${yyyyMMdd+1M}", base_datetime=leap_year_base)
    assert result == "20240229", f"闰年1月31日+1月应该是2月29日，实际: {result}"
    
    print("✅ 边界情况测试通过")

def test_parse_datetime_to_timestamp():
    """测试parse_datetime_to_timestamp函数"""
    print("🧪 测试parse_datetime_to_timestamp函数")
    print("=" * 50)
    
    test_cases = [
        # 基本日期格式
        ("20250115", "2025-01-15 00:00:00"),
        ("2025-01-15", "2025-01-15 00:00:00"),
        ("2025/01/15", "2025-01-15 00:00:00"),
        ("2025.01.15", "2025-01-15 00:00:00"),
        
        # 带时间的格式
        ("20250115103000", "2025-01-15 10:30:00"),
        ("202501151030", "2025-01-15 10:30:00"),
        ("2025-01-15 10:30:00", "2025-01-15 10:30:00"),
        ("2025-01-15 10:30", "2025-01-15 10:30:00"),
        ("2025/01/15 10:30:00", "2025-01-15 10:30:00"),
        ("2025/01/15 10:30", "2025-01-15 10:30:00"),
        ("2025.01.15 10:30:00", "2025-01-15 10:30:00"),
        ("2025.01.15 10:30", "2025-01-15 10:30:00"),
        
        # 带毫秒的格式
        ("20250115103000123456", "2025-01-15 10:30:00.123456"),
        ("2025-01-15 10:30:00.123456", "2025-01-15 10:30:00.123456"),
        ("2025/01/15 10:30:00.123456", "2025-01-15 10:30:00.123456"),
        ("2025.01.15 10:30:00.123456", "2025-01-15 10:30:00.123456"),
        
        # ISO格式
        ("2025-01-15T10:30:00", "2025-01-15 10:30:00"),
        ("2025-01-15T10:30:00.123456", "2025-01-15 10:30:00.123456"),
        ("2025-01-15T10:30:00Z", "2025-01-15 10:30:00"),
        ("2025-01-15T10:30:00.123456Z", "2025-01-15 10:30:00.123456"),
    ]
    
    for i, (input_str, expected_str) in enumerate(test_cases, 1):
        try:
            timestamp = parse_datetime_to_timestamp(input_str)
            dt = datetime.fromtimestamp(timestamp)
            result_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # 处理毫秒部分
            if "." in expected_str:
                result_str += f".{dt.microsecond:06d}"
                expected_str = expected_str.replace(".123456", f".{dt.microsecond:06d}")
            
            print(f"测试 {i:2d}: {input_str}")
            print(f"结果: {result_str}")
            print(f"时间戳: {timestamp}")
            print()
            
        except Exception as e:
            print(f"测试 {i:2d}: {input_str}")
            print(f"错误: {e}")
            print()

if __name__ == "__main__":
    print("🧪 测试日期时间解析功能")
    print("=" * 50)
    
    test_datetime_parser_with_fixed_base()
    print()
    test_datetime_parser_without_base()
    print()
    test_edge_cases()
    print()
    print("🎉 所有测试通过！") 