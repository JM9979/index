"""
Dependecies for FastAPI
"""
import aiohttp
from app.config import config
import aiomysql
import logging
import asyncio

async def call_node_rpc(method: str, params: list, if_full_response=False):
    """
    Call node RPC
    """
    data = {
        "jsonrpc": "1.0",
        "id": "curltest",
        "method": method,
        "params": params
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.TBC_RPC_URL,
                json=data, auth=aiohttp.BasicAuth(*config.TBC_RPC_AUTH)
            ) as response:

                result = await response.json()

                # if full txt response is needed
                if not if_full_response:
                    result = result["result"]

                return result
    except Exception as e:
        raise ConnectionError(f"Failed to call node RPC {method}: {str(e)}") from e


async def syclic_call_rpc(method, params):
    """
    Syclic call RPC.
    """
    retry_interval = 5
    while True:
        try:
            res = await call_node_rpc(method=method, params=params)
            return res
        except (ConnectionError, TimeoutError, ValueError) as e:
            logging.error("Error calling node RPC %s: %s. Retrying in %s seconds...", method, e, retry_interval)
            await asyncio.sleep(retry_interval)


class DBManager:
    """
    Database manager
    """
    _pool = None

    @classmethod
    async def init_pool(cls, host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER, password=config.MYSQL_PASS, db=config.MYSQL_DB):
        """
        初始化数据库连接池
        """
        cls._pool = await aiomysql.create_pool(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
            minsize=1,
            maxsize=150,
            autocommit=True
        )

    @classmethod
    async def close_pool(cls):
        """
        关闭数据库连接池
        """
        if cls._pool:
            cls._pool.close()
            await cls._pool.wait_closed()

    @classmethod
    async def execute_query(cls, query, params=None):
        """
        执行 SQL 查询
        """
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params or ())
                result = await cur.fetchall()
                return result

    @classmethod
    async def execute_update(cls, query, params=None):
        """
        执行 SQL 更新语句 (INSERT, UPDATE, DELETE)
        """
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params or ())
                await conn.commit()

                
    @staticmethod
    async def execute_update_nocommit(conn, query, params=None):
        """
        执行 SQL 更新语句但不提交 (INSERT, UPDATE, DELETE)
        """
        async with conn.cursor() as cur:
            await cur.execute(query, params or ())