import redis.asyncio as redis
import asyncio

# List:双向链表 ⽀持从两端插⼊和弹出 常⽤于简单消息队列 最新动态列表 栈

pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True,
)
redis_pool = redis.Redis(connection_pool=pool)



async def main():
    async with redis_pool as r:
        list_length = await r.rpush("queue","a","b","c") # 从右端依次插入 返回插入后的列表长度
        print(f"长度:{list_length}")

        await r.lpush("queue","x","y","z") # 从左端插入

        print(await r.lrange("queue",0,-1)) # 按下标取范围

        print(await r.lpop("queue")) # 弹出头部元素

        print(await r.rpop("queue")) # 弹出尾部元素

        print(await r.lpop("queue",5)) # 弹出前5个

        print(await r.llen("queue")) # 获取列表长度

        await r.lset("queue",0,"new") # 修改指定下标的元素

        await r.lrem("queue",0,"a") # 删除值为 a 的元素 count = 0表示全部删除

        await r.ltrim("queue",0,10) # 只保留下标 0到10 的元素



asyncio.run(main())