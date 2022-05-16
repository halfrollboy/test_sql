import psycopg2.pool as pool
import psycopg2.extras as extras
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
import psycopg2
import os
import json
from dotenv import load_dotenv
from colorama import init
from colorama import Fore

init()


# load_dotenv будет искать файл .env, и, если он его найдет,
# из него будут загружены переменные среды

# Добавлено для удобства работы с базой данных
load_dotenv()

null = "NULL"


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
        insert_values = "INSERT INTO office (id, parentid, name, type) VALUES %s"
        try:
            extras.execute_values(cur, insert_values, [*self.get_data()])
            self.db.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(Fore.RED, "Error: %s" % error)
            self.db.rollback()
        finally:
            cur.close()
        print(Fore.GREEN + "Database fill")

    # Сделать чтение данных
    def select_some(self, id):
        """Первая выборка данных"""
        insert_value = """
                WITH RECURSIVE r AS (
                SELECT id, parentid, name, type, 1 AS level
                FROM office
                WHERE id = {}

                UNION ALL

                SELECT office.id, office.parentid, office.name, office.type, r.level + 1 AS level
                FROM office
                    JOIN r
                        ON office.id = r.parentid
                ),
                main AS (
                SELECT id, parentid, name, type, 1 AS level
                FROM office
                WHERE id = (select r.id from r where r.type = 1)

                UNION ALL

                SELECT office.id, office.parentid, office.name,office.type, main.level + 1 AS level
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


if __name__ == "__main__":
    db = Office()
    # db.create_table()
    # db.fill_table()
    id = input("Введите id: ")
    if id.isdigit() == False:
        while id.isdigit() == False:
            print("Неверный тип попробуйте снова")
            id = input("Введите id: ")

    db.select_some(id)
