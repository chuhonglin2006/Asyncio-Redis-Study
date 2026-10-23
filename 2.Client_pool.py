import redis.asyncio as redis
import asyncio


# 1.直接创建(内部自动建池)
redis_pool_1 = redis.Redis(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True,
    db=0
)

# 2.从 URL 创建
redis_pool_2 = redis.from_url("redis://:123456@localhost:6379/0", decode_responses=True)

# 3.显式连接池,便于控制参数
pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    max_connections=50, # 最大连接数
    decode_responses=True,
    socket_timeout=5,            # 读写超时时间
    socket_connect_timeout=3,    # 连接超时时间
    health_check_interval=30,    # 空闲连接定期 PING 检测
)
redis_pool_3 = redis.Redis(connection_pool=pool)

# 推荐使用 async with 自动管理生命周期
async def main():
    async with redis_pool_3 as r:
        try:
            await r.ping()
            print("Redis连接成功")
        except Exception as error:
            print(f"连接失败:{error}")

asyncio.run(main())