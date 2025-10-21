import redis

try:
    r = redis.Redis(host='10.44.2.115', port=6379, password='sciyon', db=10)
    print("✅ Redis 连接成功，返回：", r.ping())
except Exception as e:
    print("❌ 连接失败：",e)