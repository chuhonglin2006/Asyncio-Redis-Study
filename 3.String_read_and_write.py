import redis.asyncio as redis
import asyncio

"""String:是最基础的类型 可存⽂本 数字或⼆进制 单个值最⼤ 512MB 常⽤于缓存 计数器 分布式锁 验证码"""

pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True,
    health_check_interval=30,
)
redis_pool = redis.Redis(connection_pool=pool)


async def main():
    async with redis_pool as r:
        await r.set("name","Tom") # 添加一个字符类型的数据 键值对 存在则覆盖

        await r.set("token","xxx",ex=60) # 60秒后过期

        await r.set("lock","1",nx=True) # 仅当不存在时设置

        await r.set("name","Mike",xx=True) # 仅当 key 存在时才写入 只更新 不新建

        name = await r.get("name") # 获取name的值 不存在返回None
        print(f"name的值是:{name}")

        remaining_time = await r.ttl("token") # 剩余秒数 -1是永不过期 -2是不存在
        print(f"剩余时间:{remaining_time}")
        await r.persist("token") # 设置永久不过期
        remaining_time_2 = await r.ttl("token")
        print(f"剩余时间:{remaining_time_2}")

        code = await r.getdel("token") # 读取后立即删除
        print(f"token:{code}")

asyncio.run(main())