import redis.asyncio as redis
import asyncio

client_pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True,
    max_connections=10
)
redis_pool = redis.Redis(connection_pool=client_pool)

async def main():
    async with redis_pool as r:
        await r.mset({"name":"Tom","age":18,"phone":"09374589"}) # 一次性写入多个key

        values = await r.mget(["name","age","phone"]) # 一次性读取多个key 不存在的为None
        print(values)

        await r.msetex({"a":1,"b":2,"c":3}) # 所有 key 都不存在时才会写入 要么全部成功 要么全部失败

        await r.append("name","_suffix") # 在字符串后面追加内容
        print(await r.get("name"))

        str_length = await r.strlen("name") # 获取字符串长度
        print(f"name长度为:{str_length}")

        result = await r.getrange("name",0,2) # 截取字符串 从 0 开始到 2 截取字符串
        print(f"截取的结果是:{result}")


        num = await r.delete("name","age","phone") # 批量删除字符串 返回删除了几个
        print(f"删除了:{num}个")
        num_2 = await r.unlink("a","b","c") # 异步删除字符串 删除比较大的key时使用好一些
        print(f"删除了:{num_2}个")


        result = await r.exists("name") # 判断是否存在 存在返回 1 不存在返回 0
        if result:
            print("存在")
        print("不存在")


asyncio.run(main())