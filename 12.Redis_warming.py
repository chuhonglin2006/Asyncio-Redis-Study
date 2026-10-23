import redis.asyncio as redis
import asyncio
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import select,Integer,String,Float
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
from sqlalchemy.orm import Mapped,mapped_column
import random

# Redis 单实例 预热


""" SQLAlchemy 表模型 """
class Base(DeclarativeBase):
    pass

class MySQLTable(Base):
    __tablename__ = "redis_study"
    user_id:Mapped[int] = mapped_column(Integer,primary_key=True,autoincrement=True)
    user_name:Mapped[str] = mapped_column(String(20),nullable=False)
    score:Mapped[float] = mapped_column(Float,nullable=False)

""" MySQL异步连接配置 """
DataBase_URL = "mysql+aiomysql://root:123456@localhost:3306/study?charset=utf8mb4"
async_engine = create_async_engine(DataBase_URL,echo=False)
async_session = async_sessionmaker(
    bind=async_engine,
    autoflush=False,
    expire_on_commit=False
)

""" Redis异步连接池与预热配置 """
pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True
)
redis_client = redis.Redis(connection_pool=pool)
RANK_KEY = "rank:score" # 排名键
USER_KEY = "user:{}" # 用户键
BATCH_SIZE = 500 # 批量处理大小
BASE_TTL = 3600 # 过期时间


async def redis_warm() -> int | None:
    """
    将 MySQL 中的数据预热到 Redis 中
    :return:写入条数
    """
    tmp_key = RANK_KEY + ":tmp" # 先写到 临时key 中 全部写完在改名为 正式key
    await redis_client.delete(tmp_key) # 删除可能残留的临时key
    total = 0 # 累计写入条数
    last_id = 0 # 上一批读到最大的 user_id 当作游标

    async with async_session() as session:
        while True:
            """循环分批读取 直到读取不到数据为止"""
            stmt = (select(MySQLTable).where(MySQLTable.user_id > last_id).order_by(
                MySQLTable.user_id
            ).limit(BATCH_SIZE))
            result = await session.execute(stmt)
            rows = result.scalars().all()
            if not rows:
                break # 没有数据了 退出循环

            async with redis_client.pipeline(transaction=False) as pipe:
                """使用 pipeline 先将多条命名攒在本地 execute 一次网络来返全发出去"""
                rank_mapping = {} # 本批次要加入的数据 {member:score}
                for user in rows:
                    rank_mapping[str(user.user_id)] = user.score  # ZSET 的 member 必须是 str/bytes 所以把 user_id 转成字符串
                    key = USER_KEY.format(user.user_id) # 拼出用户的 Hash key

                    await pipe.hset(key, mapping={"user_name": user.user_name, "score": user.score}) # 入队

                    await pipe.expire(key, BASE_TTL + random.randint(0, 600)) # 添加过期时间 防止雪崩

                await pipe.zadd(tmp_key, rank_mapping) # 将本次的{member:score}加入临时榜单

                await pipe.execute() # 发送指令

            total += len(rows) # 更新统计
            last_id = rows[-1].user_id # 更新游标 本批次最后一条 id 下批次从它后面开始查询

    if total > 0:
        await redis_client.rename(tmp_key,RANK_KEY) # 更新 key 名
    print(f"预热完成,共计:{total}条")
    return total

async def main():
    try:
        await redis_warm() # 执行预热函数
        print(f"总数:{await redis_client.zcard(RANK_KEY)}")
    except Exception as e:
        print(f"错误:{e}")
    finally: # 关闭数据库连接
        await redis_client.aclose()
        await async_engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())