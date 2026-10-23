import redis.asyncio as redis
import asyncio



""" Redis持久化 """

"""
    持久化是 Redis服务端 的能力 Python在其中扮演4种角色
    1. 查看持久化状态与配置
    2. 修改持久化配置
    3. 触发持久化动作
    4. 周期调度 备份 监控
"""

""" Redis连接配置 """
pool = redis.BlockingConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=10,
    max_connections=50,
)
redis_client = redis.Redis(connection_pool=pool)



""" 基础操作 """
async def main():
    # 查看状态

    # 获取Redis持久化信息
    info = await redis_client.info("persistence")

    print(f"RDB上次保存状态:{info["rdb_last_bgsave_status"]}")
    print(f"上次保存时间:{await redis_client.lastsave()}")
    print(f"AOF是否开启:{info["aof_enabled"]}")

    # AOF触发规则
    print(await redis_client.config_get("save"))

    # AOF开关
    print(await redis_client.config_get("appendonly"))

    # 文件存放目录
    print(await redis_client.config_get("dir"))


    # 开启RDB 900s 内修改1次 300s 内修改10次 60s 内修改 10000次 就自动快照
    await redis_client.config_set("save","900 1 300 10 60 10000")

    # 开启 AOF 每秒刷盘
    await redis_client.config_set("appendfsync","everysec")
    await redis_client.config_set("appendonly","yes")

    # 立刻手动保存一次
    await redis_client.bgsave()

    # 让配置重启后任然有效
    await redis_client.config_rewrite()

    await redis_client.aclose()





asyncio.run(main())



















































