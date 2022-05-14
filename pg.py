import psycopg2.extras as extras
from psycopg2 import Error
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
import os
import json
from dotenv import load_dotenv

# load_dotenv будет искать файл .env, и, если он его найдет,
# из него будут загружены переменные среды

# Добавлено для удобства работы с базой данных
load_dotenv()


# def corutine(func):
#     def inner(*args, **kwargs):
#         g = func(*args, **kwargs)
#         g.send(None)
#         return g

#     return inner


def get_connection():
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(
            user=os.getenv("PG_USERNAME"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            dbname=os.getenv("PG_DATABASE"),
        )
        connection.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
        return connection

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            connection.close()
            print("Соединение с PostgreSQL закрыто")


class Office:
    def __init__(self):
        self.conn = get_connection()
        self.cur = self.conn.cursor()

    def create_table(self):
        try:
            self.cur.execute(
                """
                CREATE TABLE office (
                    id INTEGER PRIMARY KEY,
                    parentid INTEGER,
                    name VARCHAR,
                    type INTEGER
                );
                """
            )
        except Exception as e:
            print("create ", e)
            self.conn.rollback()
        finally:
            self.conn.commit()
            self.cur.close()
        print("Таблица создана")

    def get_json_data(self):
        """Открытие и парсинг Json файла"""
        with open("data.json", "r", encoding="utf-8") as read_file:
            data = json.load(read_file)
            data = []
            for item in data:
                data.append((str(i) for i in item.values()))
        return data

    def fill_table(self):
        insert_values = "INSERT INTO public.office (id, parentid, name, type) VALUES %s"
        try:
            extras.execute_values(self.cur, insert_values, self.get_json_data())
            self.conn.commit()
            print("commit complit")
        except Exception as error:
            print("Error: %s" % error)
            self.conn.rollback()
            self.cur.close()
            return 1


if __name__ == "__main__":
    office = Office()
    office.create_table()
    office.fill_table()
