import json


def recursive_extraction(data):
    """Recusivly adds all data and types from data to json string

    Args:
        data (dict): data

    Returns:
        schema [String]: Schema as JSON String
    """
    schema = ""

    for key, value in data.items():

        schema += "{\n"
        schema += f'"name": "{key}",\n'

        if type(value) is dict:
            schema += f'"type" : "{type(value).__name__}",\n'
            schema += f'"fields": [{recursive_extraction(value)}]\n'
        else:
            schema += f'"type" : "{type(value).__name__}"\n'

        # dont add semicolon to last entry
        *_, last = data
        if key != last:
            schema += "},\n"
        else:
            schema += "}\n"

    return schema


def extract_schema(name, data):
    """Takes data from a dict and extracts it data and types to another dict

    Args:
        data (dict): data

    Returns:
        dict: schema
    """
    schema = '{\n "name": "'
    schema += name
    schema += '",\n"fields": ['
    schema += recursive_extraction(data)
    schema = schema[:-1]
    schema += "]}"

    try:
        return json.loads(schema)
    except Exception as e:
        print(e)
        return schema
