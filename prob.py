def arr():
    print("ar 1")
    yield 20
    print("ar 2")


def ar():
    return 10 + arr()


if __name__ == "__main__":
    print(type((1, 2, 4, 5)))
