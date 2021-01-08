def contains_struct(data):
    for entry in data:
        if type(entry) is dict:
            return True
    return False

def flatten_json(data):
    out = {}

    def flatten(data, name=''):
        if type(data) is dict:
            for key in data:
                flatten(data[key], name + key + '.')
        elif type(data) is list and contains_struct(data):
            i = 0
            for key in data:
                flatten(key, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = data

    flatten(data)
    return out