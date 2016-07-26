def greater(list1, list2):
    if not list1 or not list2:
        return False
    if len(list1) > len(list2):
        list1 = list1[:len(list2)]
    else:
        list2 = list2[:len(list1)]
    for idx in range(len(list1)):
        if list1[idx] < list2[idx]:
            return False
    return True


def less(list1, list2):
    if not list1 or not list2:
        return False
    if len(list1) > len(list2):
        list1 = list1[:len(list2)]
    else:
        list2 = list2[:len(list1)]
    for idx in range(len(list1)):
        if list1[idx] > list2[idx]:
            return False
    return True


def equal(list1, list2):
    if not list1 or not list2:
        return False
    if len(list1) > len(list2):
        list1 = list1[:len(list2)]
    else:
        list2 = list2[:len(list1)]
    for idx in range(len(list1)):
        if list1[idx] != list2[idx]:
            return False
    return True


def contains(list1, list2):
    if not list1 or not list2:
        return False
    if len(list1) > len(list2):
        list1 = list1[:len(list2)]
    else:
        list2 = list2[:len(list1)]
    for idx in range(len(list1)):
        if not isinstance(list1[idx], str) or not isinstance(list2[idx], str) or list1[idx] not in list2[idx]:
            return False
    return True

