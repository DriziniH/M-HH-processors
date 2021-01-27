import os
import json
import ast

type_conversion = {
    "str": str,
    "int": int,
    "float": float,
    "long": int,
    "bool": bool
}

def load_json_to_dict(json_string):
    try:
        return json.loads(json_string)
    except Exception as json_e:
        try:
            return ast.literal_eval(json_string)
        except Exception as ast_e:
            print("JSON ERROR: ", json_e)
            print("AST ERROR: ", ast_e)
            return {}

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
    
def convert_type(value, data_type):
    """Either returns value if dict or string, or tries to convert value based on given datatype 

    Args:
        value (any): value to be converted
        data_type (String): Data type value is going to be converted to

    Returns:
        value (any): converted value
    """

    if type(value) is list or type(value) is dict:
        return value

    conv_func = type_conversion.get(data_type)
    if not conv_func:
        logger.error(
            f"Error reading conversion type <{data_type}>. Please use one of {list(type_conversion.keys())}")
        return value

    try:
        return conv_func(value)
    except Exception as e:
        logger.error(
            f"Couldnt convert value <{value}> to type <{data_type}>: {str(e)}")
        return value


def map_field(data_raw, field, object_type):
    """ Determines field name, maps and converts value to given data type. 
    Depending on object type either creates key-value dict or scalar value for lists 

    Args:
        data_raw (dict): raw data
        field (dict): field with schema information
        object_type (dict or list): object type of overlying data type
    Returns:
        value (dict or scalar): mapped and converted data
    """
    field_name = field["name"]

    # Read correlating field if exists
    if "corr_field" in field["metadata"]:
        field_name = field["metadata"]["corr_field"]

    # Map data by field name and convert field to given datatype
    if field_name in data_raw:
        value = convert_type(data_raw[field_name], field["type"])
        if not object_type or object_type is dict:
            return {field["name"]: value}
        elif object_type is list:
            return value
    else:
        logger.error(
            f"Couldnt map a value for field <{field_name}>. Please make sure the corresponding field name matches the data or you reference the field in the 'corr_field' metadata field")
        return None


def convert_schema(schema, data_raw):
    """Takes fields from json schema and recursivly maps and converts raw data to schematised dict

    Args:
        schema (dict): schema and mapping information
        data_raw (dict): raw data

    Returns:
        data [dict]: dict with schematized data
    """

    def resolve_dict(fields, data_raw):
        data = {}
        for field in fields:
            subtype = resolve_subtype(field, data_raw, dict)
            if subtype:  # can possible be None if data cannot be mapped
                data.update(subtype)
        if not data:
            return None
        return data

    def resolve_list(fields, data_raw):
        data = []
        for field in fields:
            subtype = resolve_subtype(field, data_raw, list)
            if subtype:  # can possible be None if data cannot be mapped
                data.append(subtype)

        if not data:
            return None

        return data

    def resolve_subtype(field, data_raw, object_type=None, data=None):
        """Either maps and returns scalar values to the overlying function or recursivly resolves nested subtypes
        
        Args:
            field (dict): Field with schema information
            data_raw (dict): raw data
            object_type (dict or list): object type of overlying data type
            data (dict): data from previous recursion

        Returns:
            data [dict]: dict with schematized data
        """

        # consider recursive calls
        if(data is None):
            data = {}

        if field["type"] == "dict":
            if "fields" in field:
                sub_types = resolve_dict(field["fields"], data_raw)
            else:
                return map_field(data_raw, field, object_type)

        elif field["type"] == "list":
            if "fields" in field:
                sub_types = resolve_list(
                    field["fields"], data_raw)
            else:
                return map_field(data_raw, field, object_type)
        else:
            return map_field(data_raw, field, dict)

        if sub_types:
            data[field["name"]] = sub_types
            return data
        else:
            return None

    data = {}
    #flatten data for mapping but keep original data for direct takeover of corresponding dicts or lists
    data_raw.update(flatten_json(data_raw))

    try:
        for field in schema["fields"]:
            subtypes = resolve_subtype(field, data_raw)
            if subtypes:  # can possible be None if data cannot be mapped
                data.update(subtypes)
    except Exception as e:
        logger.error(f'Error converting schema: {str(e)}')
        return data_raw

    return data
