from pybloom_live import BloomFilter
import redis.asyncio as redis
import asyncio
from sqlalchemy import select,String,Integer,Float
from sqlalchemy.orm import DeclarativeBase,Mapped,mapped_column
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
import random


""" 缓存穿透 """

# 恶意请求或者Bug 不断查询不存在的数据 Redis中没有数据库中也没有 每次请求都会打到数据库

"""
解决方案:
        1.缓存空值:数据库查询不到时 给Redis设置一个短TTL的占位符
        2.布隆过滤器:在最前面拦截一定不存在的Key
"""

""" 数据库连接配置 """
# Redis配置
redis_client_pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True
)
redis_client = redis.Redis(connection_pool=redis_client_pool)

BASE_TTL = 600                   # 正常数据基础过期时间 10 分钟
NULL_TTL = 60                    # 空值过期时间:要短,否则数据后来被创建了,缓存还在返回空
JITTER_RATIO = 0.2               # TTL 抖动比例:±20%

def ttl_with_jitter(base: int) -> int:
    """给过期时间加随机抖动,避免大批 key 在同一秒过期(解决雪崩)"""
    return int(base * (1 + random.uniform(-JITTER_RATIO, JITTER_RATIO)))



# SQLAlchemy 表模型
class Base(DeclarativeBase):
    pass

class Users(Base):
    __tablename__ = "redis_study"
    user_id:Mapped[int] = mapped_column(Integer,primary_key=True,autoincrement=True)
    user_name:Mapped[str] = mapped_column(String(20),nullable=False)
    score:Mapped[float] = mapped_column(Float,nullable=False)

# MySQL配置
MySQL_URL = "mysql+aiomysql://root:123456@localhost:3306/study?charset=utf8mb4"
async_engine = create_async_engine(url=MySQL_URL,echo=False)
async_session = async_sessionmaker(
    bind=async_engine,
    autoflush=False,
    expire_on_commit=False,
    class_=AsyncSession
)



""" 代码示例 """

""" 方案1 缓存空值 """

async def find_user_1(user_id:int):
    key = f"user:{user_id}"

    # 1.查询Redis
    result = await redis_client.hgetall(key)
    if result:
        if result.get("_empty") == "1": # 结果是空值占位符 直接返回None
            return None
        return result # 返回结果

    # 2.Redis中没有 查询数据库
    async with async_session() as session:
        stmt = select(Users).where(Users.user_id == user_id)
        user_class = await session.execute(stmt)
        result = user_class.scalar_one_or_none()
        if result:        # 数据库中查询到了 正常缓存 TTL带抖动(防止雪崩)
            await redis_client.hset(f"user_id:{result.user_id}"
                                    ,mapping={"user_name":result.user_name,
                                            "score":str(result.score)
                                            }
                                    )
            await redis_client.expire(f"user_id:{result.user_id}",ttl_with_jitter(BASE_TTL))

            return result # 返回数据库中查询到的结果
        else:             # 数据库中没有查询到 设置缓存空值 并设置TTL
            await redis_client.hset(key,mapping={"_empty":"1"})
            await redis_client.expire(key,ttl_with_jitter(NULL_TTL))
            return None # 返回None



""" 方案2 布隆过滤器"""


# 设置布隆过滤器
user_bloom = BloomFilter(
    capacity=100000,
    error_rate=0.01
)

# 从MySQL中加载用户ID到布隆过滤器中
async def init_user_bloom():
    async with async_session() as session:
        user_class = await session.execute(
            select(Users.user_id)
        )
        result = user_class.scalars().all()
        for userid in result:
            user_bloom.add(userid) # 添加到过滤器中
        print(f"布隆过滤器初始化完成 共加载:{len(result)}个用户")

async def find_user_2(user_id:int):
    if user_id not in user_bloom: # 判断id是否在布隆过滤器中
        return None
    key = f"user:{user_id}"

    # 2. 查询 Redis Hash
    cached = await redis_client.hgetall(key)
    if cached:
        if cached.get("_empty") == "1":
            return None
        return {"user_id": int(cached["user_id"]),
                "user_name": cached["user_name"],
                "score": float(cached["score"]),
                }
    # 3. Redis 未命中，查询 MySQL
    async with async_session() as session:
        result = await session.execute(
            select(Users).where(Users.user_id == user_id)
        )
        user = result.scalar_one_or_none()

    # 4.数据库不存在：缓存空值
    if user is None:
        await redis_client.hset(key,mapping={"_empty": "1"})
        await redis_client.expire(key,ttl_with_jitter(NULL_TTL))
        return None

    # 5.数据库存在：缓存正常用户
    user_data = {"user_id": user.user_id,
                "user_name": user.user_name,
                "score": float(user.score),
                }
    await redis_client.hset(key,mapping={
            "user_id": str(user_data["user_id"]),
            "user_name": user_data["user_name"],
            "score": str(user_data["score"]),
        }
    )
    await redis_client.expire(
        key,
        ttl_with_jitter(BASE_TTL)
    )
    return user_data


async def main():
    try:
        ID_1 = int(input("请输入ID:"))
        user_1 = await find_user_1(ID_1)
        print(f"用户信息:{user_1}")

        ID_2 = int(input("请输入ID:"))
        user_2 = await find_user_2(ID_2)
        print(f"用户信息:{user_2}")

    except Exception as error:
        print(f"错误:{error}")
    finally: # 关闭数据库连接
        await redis_client.aclose()
        await async_engine.dispose()



if __name__ == "__main__":
    asyncio.run(main())







































