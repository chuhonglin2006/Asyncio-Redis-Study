import redis.asyncio as redis
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker,AsyncSession
from sqlalchemy import Integer,Float,String,select
from sqlalchemy.orm import DeclarativeBase,Mapped,mapped_column
import json,random,asyncio
from cachetools import TTLCache


""" 雪崩 """
# 同一时间大量缓存Key同时失效 或者Redis整体不可使用 导致海量请求直接打到数据库 导致数据库崩溃
""" 
    1.大量Key同时过期: 批量预热/导入数据时使用了相同的TTL 特点: 有规律 可预测 在固定时间爆发
    2.Redis宕机: 实例挂了 网络分区 主从切换 内存打满 特点: 突发 全量 所有Key同时失效
"""

"""
    解决方案:
    1.TTL加随机抖动: 打散过期时间
    2.热点数据永不过去: 热点不会集中失效
    3.分批预热: 避免一次性写入同一批TTL
    4.互斥锁:同一个Key只有一个请求回源
    5.多级缓存: Redis失效时本地缓存兜底
    6.Redis高可用: 防止Redis整体宕机
    7.兜底: 限流 + 熔断 + 降级: 保证数据库不被压垮
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

""" 雪崩防护配置 """
TTL_JITTER = 600            # TTL 随机抖动范围: 0~600 秒
NULL_TTL = 60               # 空值缓存短 TTL
LOCK_KEY = "lock:user:{}"
LOCK_TTL = 10               # 锁自动过期,防止死锁
LOCAL_TTL = 30              # 本地缓存 TTL,要比 Redis 短
DB_CONCURRENCY = 20         # 允许同时访问数据库的最大并发

local_cache = TTLCache(maxsize=10000, ttl=LOCAL_TTL)   # L1 本地缓存
db_semaphore = asyncio.Semaphore(DB_CONCURRENCY)       # 数据库并发限流

def get_ttl() -> int:
    """ 基础 TTL + 随机抖动"""
    return BASE_TTL + random.randint(0, TTL_JITTER)



""" 代码演示 """

"""
    方案1 TTL随机抖动
    原理: 把 同一时刻过期 变成 分散在一个时间窗口内过期
"""
async def set_user_cache(user_id:int,data:dict | None):
    """ 设置缓存时添加随机TTL """
    key = USER_KEY.format(user_id)
    if data is None:
        # 空值也缓存 防止穿透 TTL 短一些
        await redis_client.set(key,NULL_VALUE,ex=NULL_TTL + random.randint(0,30))
    else:
        await redis_client.set(key,json.dumps(data),ex=get_ttl())


"""
    方案2 分批预热
    原理: 预热时使用 Pipeline 批量写入以减少网络往返 同时每个Key使用独立的TTL
"""
async def warm_up_cache():
    """ 启动时预热 分批读数据库 -> Pipeline 批量写入Redis  """
    last_id = 0 # 数据库游标
    while True:
        async with async_session() as session:
            stmt = (select(User).where(User.user_id > last_id).order_by(
                User.user_id).limit(BATCH_SIZE))
            users = (await session.execute(stmt)).scalars().all()
        if not users:
            break
        pipe = redis_client.pipeline(transaction=False)
        for u in users:
            _ = pipe.set(
                USER_KEY.format(u.user_id),
                json.dumps({"user_id":u.user_id,"user_name":u.user_name,"score":u.score}),
                ex=get_ttl()
            )
        await pipe.execute()

        last_id = users[-1].user_id

        # # 给数据库喘息时间,避免预热本身打垮库
        await asyncio.sleep(0.05)

"""
    方案3 多级缓存 + Redis故障降级
    原理: 本地进程内缓存作为L1 Redis作为L2 数据库作为最后 Redis整体宕机时 本地缓存还能抗一部分热点流量
"""
async def get_user(user_id:int):
    key = USER_KEY.format(user_id)

    # L1 本地缓存
    hit = local_cache.get(key)
    if hit is not None:
        return None if hit == NULL_VALUE else hit

    # L1 中没有 读取 L2
    cached = await redis_client.get(key)
    if cached is not None:
        # 从 L2 中读取存入 L1 中
        if cached == NULL_VALUE:
            local_cache[key] = NULL_VALUE
            return None
        data = json.loads(cached)
        local_cache[key] = data
        return data
    return None

"""
    方案4 熔断
    原理: 数据库持续失败 超时时 快速失败而不是继续堆请求 给数据库恢复时间
"""














































