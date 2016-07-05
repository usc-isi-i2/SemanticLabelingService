def greater(x, y):
    return x > y


def lesser(x, y):
    return x < y


def equal(x, y):
    return x == y


def contains(x, y):
    if isinstance(x, str) and isinstance(y, str):
        return y in x
    return False
