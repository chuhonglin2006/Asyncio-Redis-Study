import redis.asyncio as redis
import asyncio

# Set:⽆序 元素唯⼀的集合 适合去重 标签 好友关系 抽奖 共同关注

pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True
)
redis_pool = redis.Redis(connection_pool=pool)



async def main():
    async with redis_pool as r:
        add_number = await r.sadd("tags:1","Python","C","MySQL","Redis") # 添加元素 重复的会被忽略
        print(f"添加了:{add_number}个元素")
        await r.sadd("tags:2","Django","FastApi","Flask","Python")

        print(await r.smembers("tags:1")) # 返回所有元素 集合 无序

        print(await r.sismember("tags:1","Python")) # 判断是否包含某个元素

        print(await r.scard("tags:1")) # 返回元素个数

        print(await r.srem("tags:1","MySQL")) # 删除元素

        print(await r.srandmember("tags:1",2)) # 随机抽取两个元素 不删除

        print(await r.spop("tags:1")) # 随机弹出一个元素 并且删除

        print(await r.sinter('tags:1','tags:2')) # 两个集合的交集

        print(await r.sunion("tags:1","tags:2")) # 并集

        print(await r.sdiff("tags:1","tags:2")) # 差集 1有 2没有的


        async for member in r.sscan_iter('tags:1', count=10):
            print(member) # 集合很⼤时不要⽤ smembers 改⽤ sscan_iter 分批遍历

asyncio.run(main())