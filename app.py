import argparse
from office import Office


def organization():
    """Функция обработки аргумента"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--create", help="use for create table", action="store_true"
    )
    args = parser.parse_args()
    return args.create


def get_id():
    """Получение id для поиска в таблице"""
    id = input("Введите id: ")
    if id.isdigit() is False:
        while id.isdigit() is False:
            print("Неверный тип попробуйте снова")
            id = input("Введите id: ")
    return id


if __name__ == "__main__":
    db = Office()
    if organization():
        db.create_table()
        db.fill_table()

    id = get_id()
    db.select_some(id)
