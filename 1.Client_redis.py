import asyncio
import redis.asyncio as redis

async def main():
    redis_client = redis.Redis(
        host="localhost", # ip地址
        port=6379, # 端口
        password="1234", # 密码
        db=0, # 选择数据库
        decode_responses=True # 返回内容解码
    )

    try:
        await redis_client.ping() # 发送ping指令 成功返回True
        print("Redis连接成功")
    except Exception as error:
        print(f"连接失败:{error}")

    await redis_client.set("1","Hello Redis") # 1是键后面的是值
    result = await redis_client.get("1") # 通过键获得值
    print(result)
    await redis_client.aclose() # 关闭redis连接

asyncio.run(main())