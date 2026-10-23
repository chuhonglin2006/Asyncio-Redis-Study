import redis.asyncio as redis
import asyncio
import json

# Json的存取与序列化


pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True
)

redis_pool = redis.Redis(connection_pool=pool)

async def main():
    async with redis_pool as r:
        user = {'id':1,'name':'Mike','tags':['a','b'],'active':True}

        await r.set("user:1",json.dumps(user,ensure_ascii=False)) # 将字典转化为字符串

        raw = await r.get("user:1")
        user_obj = json.loads(raw) if raw is not None else None # 判断是否命中 在还原成字典
        print(user_obj)


asyncio.run(main())