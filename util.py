def check_list(list, type):
    return all([isinstance(elem, type) for elem in list])