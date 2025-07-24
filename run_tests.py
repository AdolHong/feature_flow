#!/usr/bin/env python3
"""
Redis连接器测试运行脚本
"""

import subprocess
import sys

def run_tests():
    """运行所有测试"""
    print("🚀 开始运行Redis连接器测试...")
    
    # 检查是否安装了pytest
    try:
        import pytest
    except ImportError:
        print("❌ pytest未安装，正在安装...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pytest"])
        print("✅ pytest安装完成")
    
    # 运行测试
    test_commands = [
        # 运行所有测试
        [sys.executable, "-m", "pytest", "test/", "-v"],
        
        # 运行特定测试类
        # [sys.executable, "-m", "pytest", "test/test_redis_connector.py::TestRedisConnector", "-v"],
        
        # 运行性能测试
        # [sys.executable, "-m", "pytest", "test/test_redis_connector.py::TestRedisConnectorPerformance", "-v"],
    ]
    
    for cmd in test_commands:
        print(f"\n🔍 运行命令: {' '.join(cmd)}")
        try:
            result = subprocess.run(cmd, capture_output=False, text=True)
            if result.returncode != 0:
                print(f"❌ 测试失败，退出码: {result.returncode}")
                return False
        except Exception as e:
            print(f"❌ 运行测试时出错: {e}")
            return False
    
    print("\n✅ 所有测试执行完成！")
    return True

def run_coverage():
    """运行测试覆盖率"""
    print("\n📊 运行测试覆盖率分析...")
    
    try:
        import coverage
    except ImportError:
        print("❌ coverage未安装，正在安装...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "coverage"])
        print("✅ coverage安装完成")
    
    # 运行覆盖率测试
    coverage_cmd = [
        sys.executable, "-m", "coverage", "run", "-m", "pytest", "test/", "-v"
    ]
    
    try:
        subprocess.run(coverage_cmd, check=True)
        
        # 生成覆盖率报告
        subprocess.run([sys.executable, "-m", "coverage", "report", "-m"])
        subprocess.run([sys.executable, "-m", "coverage", "html"])
        
        print("\n✅ 覆盖率报告生成完成！")
        print("📄 HTML报告位置: htmlcov/index.html")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ 覆盖率测试失败: {e}")
        return False
    
    return True

def main():
    """主函数"""
    if len(sys.argv) > 1:
        if sys.argv[1] == "coverage":
            run_coverage()
        elif sys.argv[1] == "help":
            print("使用方法:")
            print("  python run_tests.py        # 运行所有测试")
            print("  python run_tests.py coverage # 运行测试覆盖率")
            print("  python run_tests.py help    # 显示帮助")
        else:
            print("❌ 未知参数，使用 'python run_tests.py help' 查看帮助")
    else:
        run_tests()

if __name__ == "__main__":
    main() 