# Redis连接器测试

本目录包含Redis连接器的完整单元测试套件。

## 测试结构

### 测试类

1. **TestRedisConnector** - 基础功能测试
   - JSON变量存储和读取
   - 直接变量存储和读取
   - TTL功能测试
   - 工具函数测试
   - 错误处理测试
   - 数据类型和序列化测试

2. **TestRedisConnectorWithJSONModule** - RedisJSON模块测试
   - JSON模块基本操作
   - 字段级操作（get_json_field, update_json_field）
   - 错误处理

3. **TestRedisConnectorPerformance** - 性能测试
   - 字符串模式 vs JSON模块性能对比
   - 存储和读取性能测试

4. **TestRedisConnectorConnection** - 连接测试
   - 连接成功测试
   - 连接失败测试
   - JSON模块连接测试

## 运行测试

### 方法1：使用pytest（推荐）

```bash
# 安装pytest
pip install pytest

# 运行所有测试
pytest test/ -v

# 运行特定测试类
pytest test/test_redis_connector.py::TestRedisConnector -v

# 运行特定测试方法
pytest test/test_redis_connector.py::TestRedisConnector::test_json_variable_operations -v
```

### 方法2：使用unittest

```bash
# 运行所有测试
cd test
python test_redis_connector.py

# 或者从项目根目录
python -m unittest test.test_redis_connector -v
```

### 方法3：使用测试脚本

```bash
# 运行所有测试
python run_tests.py

# 运行测试覆盖率
python run_tests.py coverage

# 查看帮助
python run_tests.py help
```

## 测试要求

### 必需条件
- Redis服务器运行在localhost:6379
- Python 3.7+
- 安装依赖：`pip install redis pandas numpy`

### 可选条件
- RedisJSON模块（用于高级JSON操作测试）
- pytest（用于更好的测试体验）
- coverage（用于测试覆盖率）

## 启动Redis服务器

### macOS
```bash
# 使用Homebrew安装并启动
brew install redis
brew services start redis

# 或者直接运行
redis-server
```

### Ubuntu/Debian
```bash
# 安装Redis
sudo apt-get install redis-server

# 启动服务
sudo systemctl start redis
```

### Docker
```bash
# 标准Redis
docker run -p 6379:6379 redis:latest

# 带RedisJSON模块的Redis
docker run -p 6379:6379 redislabs/rejson:latest
```

## 测试覆盖率

运行测试覆盖率分析：

```bash
# 安装coverage
pip install coverage

# 运行覆盖率测试
coverage run -m pytest test/ -v

# 生成报告
coverage report -m

# 生成HTML报告
coverage html
```

## 故障排除

### 常见问题

1. **Redis连接失败**
   ```
   redis.exceptions.ConnectionError: Error 61 connecting to localhost:6379
   ```
   - 解决：确保Redis服务器正在运行

2. **RedisJSON模块不可用**
   ```
   ⚠️ RedisJSON模块不可用，回退到字符串模式
   ```
   - 解决：使用支持RedisJSON的Redis实例

3. **导入错误**
   ```
   ModuleNotFoundError: No module named 'redis_connector'
   ```
   - 解决：确保在正确的目录运行测试

### 跳过特定测试

```bash
# 跳过需要RedisJSON的测试
pytest test/ -v -k "not JSONModule"

# 跳过性能测试
pytest test/ -v -k "not Performance"
```

## 持续集成

测试可以轻松集成到CI/CD流程中：

```yaml
# GitHub Actions示例
- name: Start Redis
  uses: supercharge/redis-github-action@1.4.0
  with:
    redis-version: 6

- name: Run tests
  run: |
    pip install pytest redis pandas numpy
    pytest test/ -v
``` 