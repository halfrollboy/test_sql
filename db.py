import psycopg2.pool as pool
import psycopg2.extras as extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
import os
import json
from dotenv import load_dotenv

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
        print(f"Нет подключения к базе \n {e}")

    def session():
        try:
            conn = pg_pool.getconn()
            # conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn
        except Exception as e:
            print(e)

    return session


class Office:
    """Представление базы"""

    def __init__(self):
        self.db = get_session()()

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
        except Exception as e:
            print("create ", e)
            self.db.rollback()
        finally:
            self.db.commit()
        print("Таблица создана")

    def none_replace(self, row):
        for var in row:
            if var == None:
                var = " "
            yield var

    def get_data(self):
        """Открытие и парсинг Json файла"""
        with open("data.json", "r", encoding="utf-8") as read_file:
            contains = json.load(read_file)
            data = []
            for item in contains:
                data.append(tuple(i for i in item.values()))
            print(data)
        return data

    def fill_table(self):
        # string = string[:-1] + ";"
        cur = self.db.cursor()
        insert_values = "INSERT INTO office (id, parentid, name, type) VALUES %s"
        try:
            extras.execute_values(cur, insert_values, [*self.get_data()])
            self.db.commit()
            print("commit complit")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.db.rollback()
            cur.close()
            return 1

        # except Exception as e:
        #     print(e)
        #     self.cur.rollback
        # finally:
        #     print("com")
        #     self.db.commit()

        # print(string)
        # try:
        #     self.cur.execute(
        #         """
        #         INSERT INTO office (id, parentid, name, type) VALUES {}
        #         """.format(
        #             string
        #         )
        #     )
        # except Exception as e:
        #     print("Fill table ", e)
        print("data okey")


if __name__ == "__main__":
    db = Office()
    db.create_table()
    db.fill_table()


#  if somecondition:
#         anotherConn.commit()
#     else:
#         anotherConn.rollback
# cursor.close()
