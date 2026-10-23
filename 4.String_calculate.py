import redis.asyncio as redis
import asyncio

client_pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True
)
redis_pool = redis.Redis(connection_pool=client_pool)

async def main():
    async with redis_pool as r:
        await r.set("views",0) # 设置一个键值对 view 为 0
        await r.set("price",1.2)

        await r.incr("views") # 增加 1
        print(await r.get("views"))

        await r.incrby("views",10) # 增加指定的数量
        print(await r.get("views"))

        await r.decr("views") # 减少 1
        print(await r.get("views"))

        await r.decrby("views",5) # 减少指定的数量
        print(await r.get("views"))

        await r.incrbyfloat("price",0.5) # 对浮点数加
        print(await r.get("price"))

        # 如果值不是 数字 会报错

asyncio.run(main())
