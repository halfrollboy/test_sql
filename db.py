import os
from psycopg2 import pool
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
from dotenv import load_dotenv
from colorama import Fore

load_dotenv()


def get_session():
    """Метод получения сессии"""
    try:
        pg_pool = pool.SimpleConnectionPool(
            5,
            10,
            user=os.getenv("PG_USERNAME"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            dbname=os.getenv("PG_DATABASE"),
        )

    except Exception as e:
        print(Fore.RED + f"Нет подключения к базе \n {e}")
        raise e

    def session():
        try:
            conn = pg_pool.getconn()
            conn.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
            return conn
        except Exception as e:
            print(e)

    return session
