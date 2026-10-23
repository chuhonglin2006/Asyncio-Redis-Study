import asyncio,time,random,secrets
from typing import Any
from pybloom_live import ScalableBloomFilter
import redis.asyncio as async_redis
from sqlalchemy import select,Integer,String,Float
from sqlalchemy.orm import DeclarativeBase,Mapped,mapped_column
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker,AsyncSession
from cachetools import TTLCache

""" 测试: Redis 预热 + 查询 + 防穿透 雪崩 击穿 + 本地缓存 + 持久化 + 保证数据一致性 """

# 三态哨兵
MISS = object()
# 空值缓存
NULL_PLACEHOLDER = "NULL"
# Lua脚本
RELEASE_LOCK_LUA = """
if redis.call('get',KEYS[1]) == ARGV[1] then
    return redis.call('del',KEYS[1])
else
    return 0
end
"""
# 数据库回源次数
db_query_count = 0
# 进程内的请求合并
inflight:dict[int,asyncio.Future] = {}




""" Redis连接配置 """
pool = async_redis.BlockingConnectionPool(
    host="localhost",
    port=6379,
    password="1234",
    decode_responses=True,
    max_connections=50
)
redis_client = async_redis.Redis(connection_pool=pool)

""" MySQL连接配置 """
class Base(DeclarativeBase):
    pass
class Products(Base):
    __tablename__ = "products"
    id:Mapped[int] = mapped_column(Integer,primary_key=True)
    name:Mapped[str] = mapped_column(String(20),nullable=False,index=True)
    price:Mapped[float] = mapped_column(Float,nullable=False)
MySQL_URL = "mysql+aiomysql://root:123456@localhost:3306/study?charset=utf8mb4"
async_engine = create_async_engine(MySQL_URL,echo=False)
async_session = async_sessionmaker(
    class_=AsyncSession,
    bind=async_engine,
    autoflush=False,
    expire_on_commit=False)

""" 本地缓存 """
local_cache = TTLCache(maxsize=10000,ttl=30)

""" 布隆过滤器 """
products_bloom = ScalableBloomFilter(
    error_rate=0.01
)

""" 布隆过滤器初始化 """
async def add_user_id_to_bloom() -> None:
    """ 将MySQL的用户id添加到布隆过滤器中 """

    total = 0 # 添加条数
    last_id = 0 # ID游标
    start_time = time.time()

    async with async_session() as session:

        # 分批添加到布隆过滤器中
        while True:
            stmt = (select(Products.id).where(
                Products.id > last_id).order_by(
                Products.id).limit(500))
            result = await session.execute(stmt)
            row = result.scalars().all()
            if not row:
                break

            # 存入布隆过滤器
            for user_id in row:
                products_bloom.add(user_id)

            await asyncio.sleep(0)

            total += len(row)
            last_id = row[-1]

    print(f"布隆过滤器加载完成,共加载:{total}条数据")
    end_time = time.time()
    print(f"布隆过滤器加载耗时:{end_time - start_time}秒")

""" Redis预热 """
async def redis_warming() -> int | None:
    """
    Redis预热
    :return:加载到Redis中的数量
    """
    total = 0 # 总写入条数
    last_id = 0 # ID游标
    start_time = time.time()

    async with async_session() as session:

        # 分批读取数据 直到读取不到为止
        while True:
            stmt = (select(Products).where(
                Products.id > last_id).order_by(
                Products.id).limit(
                500))
            result = await session.execute(stmt)
            row = result.scalars().all()
            if not row:
                break

            # 存入Redis
            async with redis_client.pipeline() as pipe:

                for product in row:

                    key = f"id:{product.id}"

                    _ = pipe.hset(key,mapping={
                        "id":product.id,
                        "name":product.name,
                        "price":product.price})

                    _ = pipe.expire(key,600 + random.randint(0,300))

                await pipe.execute()

            total += len(row)
            last_id = row[-1].id

    print(f"预热完成,共计:{total}条")
    end_time = time.time()
    print(f"耗时:{end_time - start_time}秒")
    return total

""" 格式转换 """
def parse_hash(h:dict) -> Any:
    """ 将Redis的hash格式转换为统一格式 """

    # 返回了 {}
    if not h:
        return MISS
    if h.get("null") == NULL_PLACEHOLDER:
        return None
    return {
        "id":int(h["id"]),
        "name":h["name"],
        "price":float(h["price"])
    }

""" 写入缓存 """
async def write_cache(product_id:int,product:dict | None) -> None:
    """ 写入本地缓存 + Redis缓存 """
    key = f"id:{product_id}"

    async with redis_client.pipeline(transaction=True) as pipe:
        # 保证原子操作 防止写入失败留下永不过期的 Key
        if product is None:
            # 缓存空值
            _ = pipe.hset(key,mapping={"null":NULL_PLACEHOLDER})
            _ = pipe.expire(key,60 + random.randint(0,30))
        else:
            _ = pipe.hset(key,mapping=product)
            _ = pipe.expire(key,600 + random.randint(0,300))
        await pipe.execute()
    local_cache[key] = product

""" 查询数据库 """
async def query_from_db(products_id:int) -> Any | None:
    """ 查询数据库接口 """
    global db_query_count
    db_query_count += 1

    async with async_session() as session:
        stmt = select(Products).where(Products.id == products_id)
        result = (await session.execute(stmt)).scalar_one_or_none()
        if result:
            return {"id":result.id,
                    "name":result.name,
                    "price":float(result.price
                    )}
        else:
            return None

""" 判断是否命中缓存 """
async def hit_cached(products_id:int) -> Any | None:
    """ 判断是否命中本地缓存或者Redis缓存 """

    key = f"id:{products_id}"

    # 判断是否在布隆过滤器中
    if products_id not in products_bloom:
        return None

    # 判断是否在本地缓存中 是不是为空占位符
    local_cached_result = local_cache.get(key,MISS)
    if local_cached_result is not MISS:
        return local_cached_result

    # 判断是否在Redis中 是不是为空占位符
    redis_cached_result = parse_hash(await redis_client.hgetall(key))
    if redis_cached_result is not MISS:
        # 填回本地缓存
        local_cache[key] = redis_cached_result
    return redis_cached_result

""" 查询商品接口 """
async def query_products(products_id:int) -> dict | None:
    """ 查询商品接口 """

    # 参数校验 必须为正整数
    if not isinstance(products_id,int) or isinstance(products_id,bool) or products_id <= 0:
        raise ValueError("参数不合法")

    # 已有协程在查询同一个id 直接等待结果 不再去查询
    fut = inflight.get(products_id)
    if fut is not None:
        result = await fut

        # 返回副本
        if isinstance(result,dict):
            return dict(result) if result is not None else None
        return None

    # 第一个来的注册 Future 让后来的等待
    fut = asyncio.get_running_loop().create_future()
    inflight[products_id] = fut

    key = f"id:{products_id}"
    lock_key = f"lock:{key}"
    token = secrets.token_hex(8)

    try:
        # 先检查是否命中缓存
        cache_result = await hit_cached(products_id)
        if cache_result is MISS:
            # 未命中缓存 准备抢锁
            for _ in range(100):
                got_lock = await redis_client.set(lock_key,token,nx=True,ex=10)

                # 抢到锁
                if got_lock:
                    # 二次检查
                    try:
                        redis_cache_result = parse_hash(await redis_client.hgetall(key))
                        if redis_cache_result is not MISS:
                            local_cache[key] = redis_cache_result
                            cache_result = redis_cache_result
                        else:
                            # 二次检查没有结果 准备查询数据接
                            db_result = await query_from_db(products_id)
                            await write_cache(products_id,db_result)
                            cache_result = db_result

                    finally:
                        # 释放锁
                        await redis_client.eval(RELEASE_LOCK_LUA,1,lock_key,token)

                    break

                # 没抢到锁 让出事件循环
                await asyncio.sleep(0.1)

                # 查看缓存是否已经重建

                redis_cached_result = parse_hash(await redis_client.hgetall(key))
                if redis_cached_result is not MISS:
                    local_cache[key] = redis_cached_result
                    cache_result = redis_cached_result
                    # 别人建好缓存了 跳出循环
                    break

                else:
                    raise TimeoutError(f"等待重建超时:{key}")

        # 通知所有跟随者
        fut.set_result(cache_result)

    except asyncio.CancelledError:
        # 领头协程被取消 跟随者也要被唤醒
        fut.cancel()
        raise
    except Exception as e:
        # 将异常传递给跟随者
        fut.set_exception(e)
        fut.exception()
        raise
    finally:
        # 清理
        inflight.pop(products_id,None)
    if isinstance(cache_result,dict):
        return dict(cache_result) if cache_result is not None else None
    return None


async def clear_all_cache() -> int:
    """ 删除所有商品缓存 """
    deleted = 0
    batch = []
    # scan_iter 渐进式遍历
    async for key in redis_client.scan_iter(match="id:*", count=1000):
        batch.append(key)
        if len(batch) >= 1000:
            deleted += await redis_client.unlink(*batch)   # unlink 异步删除,不阻塞
            batch.clear()
    if batch:
        deleted += await redis_client.unlink(*batch)

    local_cache.clear()
    print(f"已清除 Redis缓存{deleted}条,本地缓存已清空")
    return deleted

async def stress_test(product_id: int, concurrency: int = 100000) -> int:
    """ 并发压测  """
    global db_query_count
    db_query_count = 0

    start = time.time()
    results = await asyncio.gather(
        *(query_products(product_id) for _ in range(concurrency)),
        return_exceptions=True,
    )
    cost = time.time() - start
    valid = sum(1 for r in results if isinstance(r, dict) and "name" in r)
    none_ = sum(1 for r in results if r is None)
    bad = concurrency - valid - none_   # 既不是商品也不是 None,说明有异常或 bug
    print(f"并发 {concurrency}, 耗时 {cost:.5f}s | "
        f"商品 {valid}, None {none_}, 异常值 {bad} | 回源 {db_query_count} 次")
    assert bad == 0, next((r for r in results if isinstance(r, BaseException)), "异常值")
    return db_query_count

async def main():
    """ 主函数 """

    # 加载布隆过滤器
    await add_user_id_to_bloom()

    # 预热Redis
    await redis_warming()

    # 清空缓存
    await clear_all_cache()

    # 热点商品缓存失效 10 万并发进来
    assert await stress_test(80000) == 1

    # 布隆里有 但 DB 没有
    products_bloom.add(99999999)
    assert await stress_test(99999999) == 1

    # 布隆里没有,直接返回 None
    assert await stress_test(88888888) == 0

    # 关闭连接
    await redis_client.aclose()
    await async_engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())













































































































