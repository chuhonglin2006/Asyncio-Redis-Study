import redis.asyncio as redis
import asyncio

"""Hash:相当于⼀个字典 ⼀个 key下有多个 value 适合存对象 ⽐把 整个对象序列化成字符串更灵活 可以单独读写某个字段"""

client_pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    max_connections=10,
    decode_responses=True
)
redis_pool = redis.Redis(connection_pool=client_pool)

async def main():
    async with redis_pool as r:
        # 设置一个 key,value使用mapping
        new_field_num = await r.hset("user_id:1",mapping={"name":"Tom","age":18,"city":"Beijing"})
        print(f"新增:{new_field_num}个字段")

        await r.hset("user_id:2","name","Mike") # 也可以单个字段放入

        print(await r.hget("user_id:1","name")) # 读取单个字段
        print(await r.hmget("user_id:1",["name","age"])) # 读取多个字段
        print(await r.hgetall("user_id:1")) # 获取全部值

        await r.hincrby("user_id:1","age",10) # 对整数字段原子加指定的数
        print(await r.hget("user_id:1","age"))

        print(await r.hexists("user_id:1","name")) # 判断字段是否存在

        print(await r.hlen("user_id:1")) # 获取字段个数

        print(await r.hkeys("user_id:1")) # 获取所有字段名

        print(await r.hvals("user_id:1")) # 获取所有值

        await r.hdel("user_id:1","name","age","city") # 删除一个或者多个字段

        await r.hsetex("user_id:1",mapping={"name":"Bob","age":20,"city":"ShangHai"}) # 字段不存在时才写入


        async for field,value in r.hscan_iter("user_id:1",count=10):
            print(f"{field}:{value}",end=" ") # 字段很多时 一次性返回全部会很慢 使用 hscan_iter 分批遍历 返回 count 条



asyncio.run(main())