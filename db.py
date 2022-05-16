import os
import json
import argparse
from psycopg2 import DatabaseError
from psycopg2 import pool
from psycopg2 import extras
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
from dotenv import load_dotenv
from colorama import init
from colorama import Fore


init()

# load_dotenv будет искать файл .env, и, если он его найдет,
# из него будут загружены переменные среды
# Добавлено исключительно для удобства работы с базой данных
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

    def session():
        try:
            conn = pg_pool.getconn()
            conn.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
            return conn
        except Exception as e:
            print(e)

    return session


class Office:
    """Представление базы"""

    def __init__(self):
        self.db = get_session()()

    def __del__(self):
        """Закрываем подключение при удалении объекта"""
        self.db.close()

    def create_table(self):
        """Метод создающий таблицу в базе данных"""
        cur = self.db.cursor()
        try:
            cur.execute(
                """
                CREATE TABLE office (
                    id INTEGER PRIMARY KEY,
                    parentid INTEGER,
                    name VARCHAR,
                    type INTEGER
                );
                """
            )
            self.db.commit()
        except Exception as e:
            print("create ", e)
            self.db.rollback()
        finally:
            cur.close()
        print(Fore.GREEN + "--Table 'office' create")

    def get_data(self):
        """Открытие и парсинг Json файла"""
        with open("data.json", "r", encoding="utf-8") as read_file:
            contains = json.load(read_file)
            data = []
            for item in contains:
                data.append(tuple(i for i in item.values()))
        return data

    def fill_table(self):
        """Метод заполняющий таблицу данными"""
        cur = self.db.cursor()
        insert_values = """INSERT INTO office (id, parentid, name, type)
                            VALUES %s"""
        try:
            extras.execute_values(cur, insert_values, [*self.get_data()])
            self.db.commit()
        except (Exception, DatabaseError) as error:
            print(Fore.RED, "Error fill: %s" % error)
            self.db.rollback()
        finally:
            cur.close()
        print(Fore.GREEN + "Database fill")

    def select_some(self, id):
        """Первая выборка данных"""
        insert_value = """
                WITH RECURSIVE r AS (
                SELECT id, parentid, name, type, 1 AS level
                FROM office
                WHERE id = {}

                UNION ALL

                SELECT office.id, office.parentid, office.name,
                    office.type, r.level + 1 AS level
                FROM office
                    JOIN r
                        ON office.id = r.parentid
                ),
                main AS (
                SELECT id, parentid, name, type, 1 AS level
                FROM office
                WHERE id = (select r.id from r where r.type = 1)

                UNION ALL

                SELECT office.id, office.parentid, office.name,
                    office.type, main.level + 1 AS level
                FROM office
                    JOIN main
                        ON office.parentid = main.id
                )

                SELECT * FROM main where main.type=1 or main.type=3;""".format(
            id
        )
        cur = self.db.cursor()
        try:
            cur.execute(insert_value)
            ar = cur.fetchall()
            print(Fore.BLUE + "RESULT:")
            for row in ar:
                print(row[2], end=", ")
        except Exception as e:
            print(Fore.RED + "Select error:", e)
            self.db.rollback()
        finally:
            cur.close()


def organization():
    """Сделано для удобства работы с запросом и создания таблицы"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--create", help="use for create table", action="store_true"
    )
    args = parser.parse_args()
    return args.create


def get_id():
    """Ввод id для поиска в таблице"""
    id = input("Введите id: ")
    if id.isdigit() is False:
        while id.isdigit() is False:
            print("Неверный тип попробуйте снова")
            id = input("Введите id: ")


if __name__ == "__main__":
    db = Office()
    if organization():
        db.create_table()
        db.fill_table()

    id = get_id()
    db.select_some(id)
