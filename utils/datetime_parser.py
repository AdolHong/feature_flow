#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from datetime import datetime, timedelta
from typing import Tuple


def parse_datetime(datetime_str: str, base_datetime: datetime = None) -> str:
    """
    解析日期时间模式并返回格式化的字符串
    支持简单模式如${yyyy}、${MM}、${dd}、${HH}、${mm}、${ss}，以及原有复杂模式
    """
    # 定义支持的模式
    patterns = {
        # 简单模式
        r'\$\{yyyy([+-]\d+y)?\}': {'format': '%Y', 'units': {'y': 'years'}},
        r'\$\{MM([+-]\d+M)?\}': {'format': '%m', 'units': {'M': 'months'}},
        r'\$\{dd([+-]\d+d)?\}': {'format': '%d', 'units': {'d': 'days'}},
        r'\$\{HH([+-]\d+H)?\}': {'format': '%H', 'units': {'H': 'hours'}},
        r'\$\{mm([+-]\d+m)?\}': {'format': '%M', 'units': {'m': 'minutes'}},
        r'\$\{ss([+-]\d+s)?\}': {'format': '%S', 'units': {'s': 'seconds'}},
        # 复杂模式
        r'\$\{yyyyMMdd([+-]\d+[yMwd])?\}': {
            'format': '%Y%m%d',
            'units': {'y': 'years', 'M': 'months', 'w': 'weeks', 'd': 'days'}
        },
        r'\$\{yyyy-MM-dd([+-]\d+[yMwd])?\}': {
            'format': '%Y-%m-%d',
            'units': {'y': 'years', 'M': 'months', 'w': 'weeks', 'd': 'days'}
        },
        r'\$\{yyyyMMddHHmmss([+-]\d+[yMwHmd])?\}': {
            'format': '%Y%m%d%H%M%S',
            'units': {'y': 'years', 'M': 'months', 'w': 'weeks', 'H': 'hours', 'm': 'minutes', 'd': 'days'}
        },
        r'\$\{yyyy-MM-dd HH:mm:ss([+-]\d+[yMwHmd])?\}': {
            'format': '%Y-%m-%d %H:%M:%S',
            'units': {'y': 'years', 'M': 'months', 'w': 'weeks', 'H': 'hours', 'm': 'minutes', 'd': 'days'}
        }
    }

    def replace_datetime_pattern(match):
        pattern_str = match.group(0)
        pattern_config = None
        for regex, config in patterns.items():
            if re.match(regex, pattern_str):
                pattern_config = config
                break
        if not pattern_config:
            return pattern_str
        offset_match = re.search(r'([+-])(\d+)([yMwdHms])', pattern_str)
        now = base_datetime or datetime.now()
        if offset_match:
            operation = offset_match.group(1)
            amount = int(offset_match.group(2))
            unit = offset_match.group(3)
            if unit == 'y':
                now = now.replace(year=now.year + amount) if operation == '+' else now.replace(year=now.year - amount)
            elif unit == 'M':
                year = now.year
                month = now.month
                if operation == '+':
                    year += (month + amount - 1) // 12
                    month = ((month + amount - 1) % 12) + 1
                else:
                    year -= (amount - month + 11) // 12
                    month = ((month - amount - 1) % 12) + 1
                day = min(now.day, (now.replace(year=year, month=month + 1, day=1) - timedelta(days=1)).day)
                now = now.replace(year=year, month=month, day=day)
            elif unit == 'w':
                days = amount * 7
                now = now + timedelta(days=days) if operation == '+' else now - timedelta(days=days)
            elif unit == 'd':
                days = amount
                now = now + timedelta(days=days) if operation == '+' else now - timedelta(days=days)
            elif unit == 'H':
                hours = amount
                now = now + timedelta(hours=hours) if operation == '+' else now - timedelta(hours=hours)
            elif unit == 'm':
                minutes = amount
                now = now + timedelta(minutes=minutes) if operation == '+' else now - timedelta(minutes=minutes)
            elif unit == 's':
                seconds = amount
                now = now + timedelta(seconds=seconds) if operation == '+' else now - timedelta(seconds=seconds)
        formatted = now.strftime(pattern_config['format'])
        return formatted
    result = re.sub(r'\$\{[^}]+\}', replace_datetime_pattern, datetime_str)
    return result



def parse_datetime_to_timestamp(datetime_str: str) -> int:
    """
    解析日期时间字符串并返回时间戳
    
    Args:
        datetime_str: 日期时间字符串，支持多种格式
        
    Returns:
        int: Unix时间戳
        
    Examples:
        >>> parse_datetime_to_timestamp("20250115")
        1737043200
        
        >>> parse_datetime_to_timestamp("2025-01-15")
        1737043200
        
        >>> parse_datetime_to_timestamp("20250115103000")
        1737078600
        
        >>> parse_datetime_to_timestamp("2025-01-15 10:30:00")
        1737078600
    """
    if not datetime_str:
        raise ValueError("日期时间字符串不能为空")
    
    # 定义支持的格式
    formats = [
        # 基本日期格式
        "%Y%m%d",           # 20250115
        "%Y-%m-%d",         # 2025-01-15
        "%Y/%m/%d",         # 2025/01/15
        "%Y.%m.%d",         # 2025.01.15
        
        # 带时间的格式
        "%Y%m%d%H%M%S",     # 20250115103000
        "%Y%m%d%H%M",       # 202501151030
        "%Y-%m-%d %H:%M:%S", # 2025-01-15 10:30:00
        "%Y-%m-%d %H:%M",   # 2025-01-15 10:30
        "%Y/%m/%d %H:%M:%S", # 2025/01/15 10:30:00
        "%Y/%m/%d %H:%M",   # 2025/01/15 10:30
        "%Y.%m.%d %H:%M:%S", # 2025.01.15 10:30:00
        "%Y.%m.%d %H:%M",   # 2025.01.15 10:30
        
        # 带毫秒的格式
        "%Y%m%d%H%M%S%f",   # 20250115103000123456
        "%Y-%m-%d %H:%M:%S.%f", # 2025-01-15 10:30:00.123456
        "%Y/%m/%d %H:%M:%S.%f", # 2025/01/15 10:30:00.123456
        "%Y.%m.%d %H:%M:%S.%f", # 2025.01.15 10:30:00.123456
        
        # ISO格式
        "%Y-%m-%dT%H:%M:%S", # 2025-01-15T10:30:00
        "%Y-%m-%dT%H:%M:%S.%f", # 2025-01-15T10:30:00.123456
        "%Y-%m-%dT%H:%M:%SZ", # 2025-01-15T10:30:00Z
        "%Y-%m-%dT%H:%M:%S.%fZ", # 2025-01-15T10:30:00.123456Z
    ]
    
    # 尝试解析每种格式
    for fmt in formats:
        try:
            # 处理带毫秒的格式
            if "%f" in fmt:
                # 确保毫秒部分有6位数字
                if "." in datetime_str:
                    parts = datetime_str.split(".")
                    if len(parts) == 2:
                        main_part = parts[0]
                        ms_part = parts[1]
                        # 补齐或截断毫秒部分到6位
                        if len(ms_part) < 6:
                            ms_part = ms_part.ljust(6, "0")
                        elif len(ms_part) > 6:
                            ms_part = ms_part[:6]
                        datetime_str = f"{main_part}.{ms_part}"
            
            dt = datetime.strptime(datetime_str, fmt)
            return int(dt.timestamp())
            
        except ValueError:
            continue
    
    # 如果所有格式都失败，尝试一些特殊处理
    try:
        # 处理可能的时区信息
        if datetime_str.endswith("Z"):
            # UTC时间
            dt_str = datetime_str[:-1]
            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]:
                try:
                    dt = datetime.strptime(dt_str, fmt)
                    return int(dt.timestamp())
                except ValueError:
                    continue
        
        # 处理可能的时区偏移
        if "+" in datetime_str or "-" in datetime_str:
            # 简单处理，只取日期时间部分
            dt_str = datetime_str.split("+")[0].split("-")[0] if "+" in datetime_str else datetime_str
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                try:
                    dt = datetime.strptime(dt_str, fmt)
                    return int(dt.timestamp())
                except ValueError:
                    continue
                    
    except Exception:
        pass
    
    # 如果所有尝试都失败，抛出异常
    raise ValueError(f"无法解析日期时间字符串: {datetime_str}")

