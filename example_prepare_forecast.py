import redis
import json
import os

# --- Redis 连接配置 ---
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_PASSWORD = None  # 如果本地Redis没有密码，设置为 None
REDIS_DB = 0           # 数据库索引

# --- 导入文件配置 ---
INPUT_JSON_FILE = "redis_export.json" # 你的JSON文件路径

# --- 1. 初始化 Redis 连接 ---
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
    r.ping()
    print(f"Successfully connected to local Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ConnectionError as e:
    print(f"Could not connect to Redis: {e}")
    exit(1)

# --- 2. (可选但推荐) 清空目标 Redis 数据库 ---
# 如果你不确定目标Redis是否干净，或者你想覆盖现有数据，可以取消注释下面这行。
# 警告：这将删除目标数据库中的所有数据！
# r.flushdb()
# print(f"Redis database {REDIS_DB} has been flushed.")

# --- 3. 读取 JSON 文件 ---
if not os.path.exists(INPUT_JSON_FILE):
    print(f"Error: JSON file not found at {INPUT_JSON_FILE}")
    exit(1)

try:
    with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f:
        imported_data = json.load(f)
    print(f"Successfully loaded data from {INPUT_JSON_FILE}. Total keys to import: {len(imported_data)}")
except json.JSONDecodeError as e:
    print(f"Error decoding JSON file: {e}")
    exit(1)
except IOError as e:
    print(f"Error reading JSON file: {e}")
    exit(1)

# --- 4. 遍历数据并写入 Redis ---
print("Starting data import to Redis...")
imported_count = 0
error_count = 0

# 使用 Redis pipeline 提高写入效率
pipe = r.pipeline()

for item in imported_data:
    key = item.get("key")
    key_type = item.get("type")
    value = item.get("value")
    ttl_info = item.get("ttl") # 可能是 "No expiration" 或 "X seconds"

    if key is None or key_type is None:
        print(f"Skipping malformed item: {item}")
        error_count += 1
        continue

    try:
        if key_type == 'string':
            pipe.set(key, value)
        elif key_type == 'hash':
            if isinstance(value, dict):
                # HMSET (or HSET in newer redis-py/Redis) 接受字典
                pipe.hmset(key, value)
            else:
                print(f"Warning: Hash value for key '{key}' is not a dictionary. Skipping hash import.")
                error_count += 1
                continue
        elif key_type == 'list':
            if isinstance(value, list):
                # LPUSH 可以一次性添加多个元素，但通常反向导入会用RPUSH
                # 这里假设导出时是按照LRANGE 0 -1 顺序，所以我们RPUSH
                if value: # 只有当列表非空时才尝试添加
                    pipe.rpush(key, *value)
            else:
                print(f"Warning: List value for key '{key}' is not a list. Skipping list import.")
                error_count += 1
                continue
        elif key_type == 'set':
            if isinstance(value, list):
                if value:
                    pipe.sadd(key, *value)
            else:
                print(f"Warning: Set value for key '{key}' is not a list. Skipping set import.")
                error_count += 1
                continue
        elif key_type == 'zset':
            if isinstance(value, list):
                if value:
                    # 使用字典来存储成员-分数对，这是更现代的redis-py用法
                    mapping = {}
                    valid_zset = True
                    for zset_item in value:
                        if 'score' in zset_item and 'member' in zset_item:
                            member = zset_item['member']
                            score = zset_item['score']
                            # Redis的ZSET成员是字符串，分数是浮点数
                            mapping[member] = float(score)
                        else:
                            print(f"Warning: Malformed zset item in key '{key}'. Skipping this zset.")
                            valid_zset = False
                            break # 跳过当前zset的导入

                    if valid_zset:
                        # 使用 mapping 参数，这在 redis-py 4.0+ 版本中是推荐的做法
                        # 它将键-分数对作为一个字典传递
                        # 如果是旧版本，这可能不起作用，但新版本可以处理任意数量的键值对
                        pipe.zadd(key, mapping)
            else:
                print(f"Warning: ZSet value for key '{key}' is not a list. Skipping zset import.")
                error_count += 1
                continue
        else:
            print(f"Warning: Unsupported key type '{key_type}' for key '{key}'. Skipping import.")
            error_count += 1
            continue

        # 设置过期时间 (TTL)
        if ttl_info and ttl_info != "No expiration":
            try:
                # 从 "X seconds" 提取数字
                ttl_seconds_str = ttl_info.split(' ')[0]
                ttl_seconds = int(ttl_seconds_str)
                if ttl_seconds > 0:
                    pipe.expire(key, ttl_seconds)
            except ValueError:
                print(f"Warning: Could not parse TTL '{ttl_info}' for key '{key}'. Skipping TTL setting.")

        imported_count += 1
        
        # 每处理一定数量的键，执行一次 pipeline，避免 pipeline 过大
        if imported_count % 1000 == 0:
            pipe.execute()
            pipe = r.pipeline() # 重置 pipeline
            print(f"Processed {imported_count} keys so far...")

    except Exception as e:
        print(f"Error importing key '{key}' ({key_type}): {e}")
        error_count += 1

# 执行剩余的 pipeline 命令
pipe.execute()

print(f"\nImport process finished.")
print(f"Total keys processed: {imported_count + error_count}")
print(f"Successfully imported: {imported_count} keys.")
print(f"Errors encountered: {error_count} keys.")
