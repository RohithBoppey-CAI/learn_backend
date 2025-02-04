import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed

import asyncpg  # type: ignore

from settings import settings
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.asyncio import (
    async_scoped_session,
    async_sessionmaker,
    create_async_engine,
)
# from static import POSTGRES_MAX_CONNECTIONS_ERROR


# POSTGRES_MAX_CONNECTIONS_ERROR = 'too many connections error'

# settings = {
#     POSTGRES_USER: "postgres",
#     POSTGRES_PASSWORD: "postgres",
#     POSTGRES_SERVICE: "localhost",
#     POSTGRES_DB: "danny_diner",
#     POSTGRES_PORT: "5432"
#   }

SQLALCHEMY_DATABASE_URL = (
    "postgresql://"
    + settings.POSTGRES_USER
    + ":"
    + settings.POSTGRES_PASSWORD
    + "@"
    + settings.POSTGRES_SERVICE
    + ":"
    + settings.POSTGRES_PORT
    + "/"
    + settings.POSTGRES_DB
)
SQLALCHEMY_DATABASE_URL_ASYNC = (
    "postgresql+asyncpg://"
    + settings.POSTGRES_USER
    + ":"
    + settings.POSTGRES_PASSWORD
    + "@"
    + settings.POSTGRES_SERVICE
    + ":"
    + settings.POSTGRES_PORT
    + "/"
    + settings.POSTGRES_DB
)

postgres_engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
)


def create_async_factory(url):
    postgres_engine_async = create_async_engine(
        url,
        pool_size=settings.POSTGRES_POOL_SIZE,  # Number of connections that can be used by the application
        max_overflow=settings.POSTGRES_MAX_OVERFLOW,  # Max nu of additional connections that can be created
    )

    async_session_factory = async_sessionmaker(
        expire_on_commit=False, bind=postgres_engine_async
    )

    meta = MetaData()
    meta.bind = postgres_engine_async

    async_session_db = async_scoped_session(
        session_factory=async_session_factory, scopefunc=asyncio.current_task
    )

    return async_session_db


async def psql_session():
    retry_count = 0
    while retry_count <= 2:
        sessionfactory = create_async_factory(SQLALCHEMY_DATABASE_URL_ASYNC)
        session = sessionfactory()
        try:
            yield session
            await session.commit()
            await session.close()
            break
        except asyncpg.exceptions.TooManyConnectionsError as e:
            print(POSTGRES_MAX_CONNECTIONS_ERROR, e)
            retry_count += 1
            if retry_count > 2:
                retry_count = 0
                await session.close()
                raise e
            await asyncio.sleep(2)
        except Exception as e:
            await session.close()
            raise e


async def psql_execute_multiple(query_list):
    sessionfactory = create_async_factory(SQLALCHEMY_DATABASE_URL_ASYNC)
    session = sessionfactory()
    with ThreadPoolExecutor(max_workers=20) as executor:
        # Pair each query with its index
        futures = {
            executor.submit(session.execute, query): index
            for index, query in enumerate(query_list)
        }
        result = [None] * len(
            query_list
        )  # Prepare a list to store results in the original order

        for future in as_completed(futures):
            try:
                index = futures[future]  # Get the index of the completed future
                result[index] = (
                    await future.result()
                ).fetchall()  # Store the result in the correct position
            except Exception as e:
                await session.close()
                print("Error in fetching data: ", e)
                raise e

        await session.close()
        return result


async def psql_execute_single(query):
    retry_count = 0
    while retry_count <= 2:
        sessionfactory = create_async_factory(SQLALCHEMY_DATABASE_URL_ASYNC)
        session = sessionfactory()
        try:
            result = (await session.execute(query)).fetchall()
            await session.close()
            break
        except asyncpg.exceptions.TooManyConnectionsError as e:
            print(POSTGRES_MAX_CONNECTIONS_ERROR, e)
            retry_count += 1
            if retry_count > 2:
                retry_count = 0
                await session.close()
                raise e
            await asyncio.sleep(2)

        except Exception as e:
            await session.close()

            raise e

    return result


async def psql_execute(query):
    retry_count = 0
    while retry_count <= 2:
        sessionfactory = create_async_factory(SQLALCHEMY_DATABASE_URL_ASYNC)
        session = sessionfactory()
        try:
            await session.execute(query)
            await session.commit()
            await session.close()
            break
        except asyncpg.exceptions.TooManyConnectionsError as e:
            print(POSTGRES_MAX_CONNECTIONS_ERROR, e)
            retry_count += 1
            if retry_count > 2:
                retry_count = 0
                await session.close()
                raise e
            await asyncio.sleep(2)
        except Exception as e:
            await session.close()
            raise e


