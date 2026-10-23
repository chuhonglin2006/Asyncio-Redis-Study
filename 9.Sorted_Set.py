import redis.asyncio as redis
import asyncio

# Sorted Set 的每一个成员关联一个分数 适合排行榜 延迟队列 按时间排序的数据

pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True
)
redis_pool = redis.Redis(connection_pool=pool)

async def main():
    async with redis_pool as r:
        await r.zadd("rank",mapping={"alice":100,"mike":90,"lily":94,"bob":80,"Tom":70}) # 传入数据 如果已经存在则更新

        print(await r.zrange("rank",0,-1,withscores=True)) # 按照分数 返回全部元素 with scores表示返回分数

        print(await r.zscore("rank","Tom")) # 查询成员分数
        print(await r.zrank("rank","bob")) # 按升序的排名
        print(await r.zrevrank("rank","alice")) # 按降序的排名

        await r.zincrby("rank",20,"Tom") # 给成员加分

        print(await r.zrangebyscore("rank",90,100,withscores=True)) # 查询在 90 到 100 分的成员

        print(await r.zcard("rank")) # 查询成员总数
        print(await r.zcount("rank",90,100)) # 分数区间内的成员总数

        await r.zrem("rank","Tom") # 删除成员

        await r.zremrangebyscore("rank",90,100) # 删除分数区间内的成员



asyncio.run(main())