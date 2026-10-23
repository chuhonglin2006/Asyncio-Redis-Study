import json,random,time,asyncio,secrets
import redis.asyncio as redis
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker,AsyncSession
from sqlalchemy import select,Integer,Float,String,update
from sqlalchemy.orm import DeclarativeBase,Mapped,mapped_column


""" 缓存击穿 """

# 一个热点Key过期的瞬间,成千上万个并发请求同时发现Redis为空值,全部去查询数据库

""" 
解决方案:
        1.互斥锁:只让一个请求去查询数据库,并重建缓存,其它请求等待或者重试读取缓存
        2.逻辑过期:Redis中的数据不设置过期时间,而是在数据内部保存一个过期时间
        3.缓存预热与主动刷新:在商品过期之前,提前查询数据库并更新缓存
        4.热点数据永不过期:对少量特别热门,更新不频繁的数据可以不设置TTL,改为在数据变更时主动更新缓存
"""


""" Redis配置 """
pool = redis.BlockingConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True,
    socket_timeout=2, # 读写超时 防止Redis卡死
    socket_connect_timeout=2, # 连接超时
    max_connections=50,
)
redis_client = redis.Redis(connection_pool=pool)
USER_KEY = "user:{}"
BATCH_SIZE = 500
BASE_TTL = 3600
NULL_VALUE = "__NULL__"

""" MySQL配置 """
class Base(DeclarativeBase):
    pass
class User(Base):
    __tablename__ = "redis_study"
    user_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_name: Mapped[str] = mapped_column(String(20), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
MySQL_URL = "mysql+aiomysql://root:123456@localhost:3306/study?charset=utf8mb4"
async_engine = create_async_engine(url=MySQL_URL, echo=False)
async_session = async_sessionmaker(
    class_=AsyncSession,
    bind=async_engine,
    autoflush=False,
    expire_on_commit=False,
)

""" 公告工具函数 """
DB_CALL_COUNT = 0 # 统计数据库被查询了多少次

def ttl_with_jitter(base:int = BASE_TTL):
    """ TTL随机加 0~300 秒的抖动 防止雪崩 """
    return base + random.randint(0,300)

async def query_user_from_db(user_id:int):
    """ 查询单个用户 """
    global DB_CALL_COUNT
    DB_CALL_COUNT += 1
    await asyncio.sleep(0.5) # 模拟慢查询
    async with async_session() as session:
        result = await session.execute(select(User).where(User.user_id == user_id))
        user = result.scalar_one_or_none()
        if user is None:
            return None
        return {"user_id":user.user_id,"user_name":user.user_name,"score":user.score}

async def query_users_batch(user_ids:list[int]):
    """ 批量查询 """
    global DB_CALL_COUNT
    DB_CALL_COUNT += 1
    async with async_session() as session:
        result = await session.execute(select(User).where(User.user_id.in_(user_ids)))
        return {
            u.user_id: {"user_id": u.user_id, "user_name": u.user_name, "score": u.score}
            for u in result.scalars().all()
        }

def decode_cache(val: bytes | str):
    """统一解析缓存值:占位符返回 None,否则反序列化"""
    return None if val == NULL_VALUE else json.loads(val)



""" 代码演示 """


""" 方案1 互斥锁 """
"""
    思路: 缓存过期后,谁先抢到锁谁去查询数据库 重建缓存 没抢到的等一会在读取缓存
    Lua脚本: 只有锁里存的 token 等于自己的 token 才删除
    判断 + 删除 必须在Redis中原子完成 否则可能出现:
    我的锁恰好过期 -> 别人拿到了锁 -> 我执行 DEL,把别人的锁误删了
    优点:强一致 实现通用
    缺点:重建期间其他请求要等待
"""
RELEASE_LOCK_LUA = """
if redis.call('get',KEYS[1]) == ARGV[1] then
    return redis.call('del',KEYS[1])
else
    return 0
end
"""

async def get_user_1(user_id_1:int):
    key = USER_KEY.format(user_id_1)
    lock_key = f"lock:{key}"
    token = secrets.token_hex(8) # 每个协程独有的锁标识 释放锁时用来验证 这是我的锁

    #  1.第一次读缓存 命中直接返回
    cached = await redis_client.get(key)
    if cached is not None:
        return decode_cache(cached)

    # 2.未命中缓存 循环尝试抢锁
    for _ in range(100):
        # SET nx ex:key 不存在才抢锁成功 并带 10秒防止死锁
        # 如果持锁的协程崩溃了 10 秒后锁会自动消失 不会永远卡住
        got_lock = await redis_client.set(lock_key,token,nx=True,ex=10)
        if got_lock:
            try:
                # 3.双重检查 抢到锁后必须在读一次缓存
                # 因为排队期间 前一个持锁的可能已经把缓存建好了
                cached = await redis_client.get(key)
                if cached is not None:
                    return decode_cache(cached)

                # 4.查询数据库 这一刻只有一个协程会执行到此处
                data = await query_user_from_db(user_id_1)

                # 5.写回缓存
                if data is None:

                    # 数据库没有 使用空值占位符
                    await redis_client.set(key,NULL_VALUE,ex=60)
                else:
                    await redis_client.set(key,json.dumps(data),ex=ttl_with_jitter())
                return data
            finally:
                # 6. 无论成功还是异常 都要释放锁
                await redis_client.eval(RELEASE_LOCK_LUA,1,lock_key,token)

        # 没抢到锁 使用sleep 让出事件循环
        await asyncio.sleep(0.1)

        # sleep 后 看看缓存是否已经被别人重建好了
        cached = await redis_client.get(key)
        if cached is not None:
            return decode_cache(cached)

    # 7.等待 10 秒还没有结果  直接查询数据库
    return await query_user_from_db(user_id_1)

""" 方案2 逻辑过期 """
"""
    思路: Redis中的key不设置TTL 过期时间存在 value 内部 发现逻辑过期后 立刻返回旧数据 同时让一个后台任务去刷新
    优点:响应最快 完全不阻塞
    缺点:重建完成前有短暂的旧数据 key 需要提前预热。
"""
LOGICAL_TTL = 600  # 逻辑过期时间:10 分钟
_background_tasks: set[asyncio.Task] = set()

async def set_logical_cache(user_id:int,data:dict,logical_ttl:int = LOGICAL_TTL):
    """ 写入带逻辑过期时间的缓存  """
    wrapper = {
        "data":data, # 业务数据
        "expire_at":time.time() + logical_ttl #   逻辑过期时间戳
    }
    await redis_client.set(USER_KEY.format(user_id),json.dumps(wrapper))

async def rebuild_cache_task(user_id:int,lock_key:str,token:str):
    """ 后台重建任务: 查库 -> 写新缓存 -> 释放锁 """
    try:
        data = await query_user_from_db(user_id)
        if data is not None:
            await set_logical_cache(user_id,data)
    except Exception as e:
        print(f"重建失败:{e}")
    finally:
        # 重建失败 或者 成功都要释放锁
        await redis_client.eval(RELEASE_LOCK_LUA,1,lock_key,token)

async def get_user_2(user_id_2:int):
    key = USER_KEY.format(user_id_2)
    lock_key = f"lock:{key}"

    # 1.读取缓存
    cached = await redis_client.get(key)
    if cached is None:
        return None
    wrapper = json.loads(cached)
    data,expire_at = wrapper["data"],wrapper["expire_at"]

    # 2.没过期 直接返回
    if expire_at > time.time():
        return data

    # 3.已逻辑过期 尝试抢锁 抢到的人重建缓存
    token = secrets.token_hex(8)
    got_lock = await redis_client.set(lock_key,token,nx=True,ex=10)

    if got_lock:
        # 4.双重检查
        cached = await redis_client.get(key)
        if cached is not None:
            wrapper = json.loads(cached)
            if wrapper["expire_at"] > time.time():
                await redis_client.eval(RELEASE_LOCK_LUA,1,lock_key,token)
                return wrapper["data"]

        # 5.确实过期 开一个后台任务去重建 当前请求不等它
        # 锁交给后台任务 由它重建后释放
        task = asyncio.create_task(rebuild_cache_task(user_id_2, lock_key, token))
        _background_tasks.add(task)

        # 完成后自动移出集合
        task.add_done_callback(_background_tasks.discard)
    return data

""" 方案3 缓存预热与主动刷新 """
"""
    思路: 不等Key过期 后台任务在过期前就把它刷新掉 读请求永远命中缓存
    优点:读路径极简单 几乎不回源
    缺点:要提前知道哪些是热点 而且要维护后台任务。
"""
HOT_USER_IDS = [1,2,3] # 假设前3个用户是热点Key 实际可以从配置 统计 或者排行榜获取
REFRESH_INTERVAL  = BASE_TTL // 2 #     刷新间隔时间 TTL的一半 保证过期前一定会刷新

async def refresh_hot_users(user_ids:list[int]):
    """ 批量刷新热点Key """
    # 按 BATCH_SIZE 分批 避免一次 in 查询或者 pipeline 过大
    for index in range(0,len(user_ids),BATCH_SIZE):

        # 切片分批处理
        batch = user_ids[index:index + BATCH_SIZE]

        # 一条 SQL 查一批 而不是循环查 N 次
        users = await query_users_batch(batch)

        # pipeline 把多条 set 一次性发给 Redis 减少网络往返
        # transaction=False 不需要事务性能更好
        async with redis_client.pipeline(transaction=False) as pipe:
            for uid in batch:
                data = users.get(uid)
                if data is None:
                    _ = pipe.set(USER_KEY.format(uid), NULL_VALUE, ex=60)
                else:
                    # TTL 添加随机抖动 防止雪崩
                    _ = pipe.set(USER_KEY.format(uid),json.dumps(data),ex=ttl_with_jitter())
            await pipe.execute()

async def refresh_loop(user_ids: list[int]):
    """后台定时刷新循环 """
    while True:
        try:
            # 多实例部署时,用一个短期锁保证同一轮只有一个实例在刷新,避免重复查库
            got = await redis_client.set("lock:refresh_hot_users", "1",nx=True, ex=REFRESH_INTERVAL - 60)
            if got:
                await refresh_hot_users(user_ids)
        except asyncio.CancelledError:
            # 应用关闭时的取消信号必须向上抛,不能吞掉
            raise
        except Exception as e:
            # 刷新失败不能让循环退出,否则以后再也不会刷新
            print(f"刷新热点缓存失败: {e}")
        await asyncio.sleep(REFRESH_INTERVAL)


async def get_user_3(user_id_3: int):
    cached = await redis_client.get(USER_KEY.format(user_id_3))
    if cached is not None:
        # 正常情况:100% 走这里,零回源
        return decode_cache(cached)

    # 兜底:非热点用户,或后台刷新失败导致 key 过期,退回互斥锁方案
    # 方案 3 写入的是普通 JSON + TTL,格式和方案 1 兼容,所以可以直接复用
    return await get_user_1(user_id_3)

""" 方案4 热点数据永不过期 """
"""
    思路:对极热点 更新少的数据 不设置 TTL 数据库一改 就同步改缓存 
    优点:最简单 性能最好
    缺点:要保证所有写入路径都更新缓存 漏一处就脏数据 并发更新时可能有顺序问题
"""
async def get_user_4(user_id_4:int):
    key = USER_KEY.format(user_id_4)

    cached = await redis_client.get(key)
    if cached is not None:
        return decode_cache(cached) # 永不过期 正常都命中缓存

    # 兜底: 这里只有第一次访问 或者 Redis重启 / 数据被淘汰到这里
    data = await query_user_from_db(user_id_4)
    if data is not None:
        await redis_client.set(key,json.dumps(data)) # 没有ex永不过期
    return data

async def update_user_score(user_id:int,new_score:float):
    """ 数据变更时主动更新缓存 没有这个函数 缓存永远都会是旧的 """

    # 1.先更新数据库
    async with async_session() as session:
        await session.execute(
            update(User).where(User.user_id == user_id).values(score=new_score)
        )
        await session.commit() # 提交新数据

    # 2.数据库更新成功后 在重建一次并且覆盖缓存
    data = await query_user_from_db(user_id)
    if data is not None:
        await redis_client.set(USER_KEY.format(user_id),json.dumps(data))




async def main():
    global DB_CALL_COUNT
    N = 1000  # 并发请求数

    """ 互斥锁 """
    await redis_client.delete("user:1", "lock:user:1")
    DB_CALL_COUNT = 0
    await asyncio.gather(*[get_user_1(1) for _ in range(N)])
    print(f"{N}个并发,数据库查询:{DB_CALL_COUNT}次")

    """ 逻辑过期 """
    await set_logical_cache(1, {"user_id": 1, "user_name": "旧数据", "score": 0}, logical_ttl=-1)
    await redis_client.delete("lock:user:1")
    DB_CALL_COUNT = 0
    results = await asyncio.gather(*[get_user_2(1) for _ in range(N)])
    print(f"并发请求拿到的是旧数据:{results[0]['user_name']}")
    await asyncio.sleep(1)  # 等后台任务重建完成
    print(f"数据库查询:{DB_CALL_COUNT}次,重建后读到:{(await get_user_2(1))['user_name']}")

    """ 主动刷新 """
    await refresh_hot_users([1])  # 预热
    DB_CALL_COUNT = 0
    await asyncio.gather(*[get_user_3(1) for _ in range(N)])
    print(f"数据库查询:{DB_CALL_COUNT}次")

    """ 永不过期 """
    await update_user_score(1, 88.8)
    DB_CALL_COUNT = 0
    results = await asyncio.gather(*[get_user_4(1) for _ in range(N)])
    print(f"数据库查询:{DB_CALL_COUNT}次,最新分数: {results[0]['score']}")
    print(f"key 的 TTL = {await redis_client.ttl('user:1')}")

    # 关闭数据库连接
    await redis_client.aclose()
    await async_engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())


















































