import redis.asyncio as redis
import asyncio

# Key的管理


pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True
)

redis_pool = redis.Redis(connection_pool=pool)

async def main():
    async with redis_pool as r:
        await r.set("score","100")
        await r.hset("key:1",mapping={"name":"alice","score":100,"age":18})
        await r.rpush("list","a","b","c","d")
        await r.sadd("set","1","2","3","4","5")
        await r.zadd("scores",mapping={"alice":100,"mike":90,"tom":80})


        print(await r.type("score")) # 获取 key 的数据类型

        await r.rename("score","year") # 重命名 key

        print(await r.randomkey()) # 随机返回一个key

        print(await r.dbsize()) # 返回当前库 key 总数

        print(await r.memory_usage("scores")) # 获取 key 的大小 字节


asyncio.run(main())











