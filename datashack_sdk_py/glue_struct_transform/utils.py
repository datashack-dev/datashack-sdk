import json
from datetime import datetime, date
from typing import Any, Tuple


def working_with_objects(item: str, jsonSchemaLoadProp: dict, isArray: bool = False) -> str:
    """
    This function works to provide a return in the glue structure format when the data type is specified as an object, further allowing the inclusion of objects in the object and arrays within it.
    """
    if isArray == True:
        jsonSchemaObjectProp = jsonSchemaLoadProp[f'{item}']['items']
        returnStruct = f"struct<"
    else:
        jsonSchemaObjectProp = jsonSchemaLoadProp[f'{item}']
        returnStruct = f"{item}:struct<"

    for values in jsonSchemaObjectProp['properties']:
        objectStruct = ""
        if 'number' in jsonSchemaObjectProp['properties'][values]['type']:
            objectStruct = f"{values}:double,"
        elif 'string' in jsonSchemaObjectProp['properties'][values]['type']:
            objectStruct = f"{values}:string,"
        elif 'integer' in jsonSchemaObjectProp['properties'][values]['type']:
            objectStruct = f"{values}:int,"
        elif 'boolean' in jsonSchemaObjectProp['properties'][values]['type']:
            objectStruct = f"{values}:boolean,"
        elif 'object' in jsonSchemaObjectProp['properties'][values]['type']:
            objectStruct = working_with_objects(values, jsonSchemaObjectProp['properties'])
        elif 'array' in jsonSchemaObjectProp['properties'][values]['type']:
            objectStruct = working_with_arrays(values, jsonSchemaObjectProp['properties'])
        elif 'null' == jsonSchemaLoadProp['properties'][values]['type']:
            raise Exception("Null is not acceptable as a schema type on glue schema.")
        else:
            pass

        returnStruct += objectStruct
    returnStruct = returnStruct[:-1] + ">,"

    return returnStruct


def working_with_arrays(item: str, jsonSchemaLoadProp: dict) -> str:
    """
    This function works to provide a return in the glue structure format when the data type is specified as array, further allowing the inclusion of objects in the array and other arrays within it.
    """
    returnArrayStruct = f"{item}:array<"
    jsonSchemaArray = jsonSchemaLoadProp[f'{item}']['items']
    if 'number' in jsonSchemaArray['type']:
        arrayStruct = f"double,"
    elif 'string' in jsonSchemaArray['type']:
        arrayStruct = f"string,"
    elif 'integer' in jsonSchemaLoadProp[f'{item}']['type']:
        arrayStruct = f"int,"
    elif 'boolean' in jsonSchemaArray['type']:
        arrayStruct = f"boolean,"
    elif 'null' == jsonSchemaLoadProp[f'{item}']['type']:
        raise Exception("Null is not acceptable as a schema type on glue schema.")
    elif 'object' in jsonSchemaArray['type']:
        arrayStruct = working_with_objects(item, jsonSchemaLoadProp, True)
    elif 'array' in jsonSchemaArray['type']:
        arrayStruct = working_with_arrays(item, jsonSchemaLoadProp)
    else:
        pass

    returnArrayStruct += arrayStruct[:-1] + ">,"
    return returnArrayStruct


def working_with_types(item: str, jsonSchemaLoadProp: dict) -> str:
    """
    This function performs the basic handling for simple data types like number, string, integer... And for more complex types like array and object it asks for the help of another function.
    """
    returnStruct = ""
    if 'number' in jsonSchemaLoadProp[f'{item}']['type']:
        returnStruct = f"{item}:double,"
    elif 'string' in jsonSchemaLoadProp[f'{item}']['type']:
        returnStruct = f"{item}:string,"
    elif 'integer' in jsonSchemaLoadProp[f'{item}']['type']:
        returnStruct = f"{item}:int,"
    elif 'boolean' in jsonSchemaLoadProp[f'{item}']['type']:
        returnStruct = f"{item}:boolean,"
    elif 'null' == jsonSchemaLoadProp[f'{item}']['type']:
        raise Exception("Null is not acceptable as a schema type on glue schema.")
    elif 'object' in jsonSchemaLoadProp[f'{item}']['type']:
        returnStruct = working_with_objects(item, jsonSchemaLoadProp)
    elif 'array' in jsonSchemaLoadProp[f'{item}']['type']:
        returnStruct = working_with_arrays(item, jsonSchemaLoadProp)
    else:
        pass

    return returnStruct


def working_with_objects_json_body(key: str, item: str, jsonBodyLoadProp: dict, isArray: bool = False) -> Tuple[
    str, Any]:
    """
    This function works to provide a return in the glue structure format when the data type is specified as an object, further allowing the inclusion of objects in the object and arrays within it.
    """

    if isArray == True:
        jsonObjectProp = jsonBodyLoadProp[0]
        returnStruct = f"struct<"
    else:
        jsonObjectProp = jsonBodyLoadProp
        returnStruct = f"{key}:struct<"

    serializable_values_d = {}

    for key, value in jsonObjectProp.items():
        objectStruct = ""
        if 'int' in str(type(value)):
            objectStruct = f"{key}:int,"
            serializable_val = value
        elif 'float' in str(type(value)):
            objectStruct = f"{key}:double,"
            serializable_val = value
        elif 'str' in str(type(value)):
            objectStruct = f"{key}:string,"
            serializable_val = value
        elif 'bool' in str(type(value)):
            objectStruct = f"{key}:boolean,"
            serializable_val = value
        elif isinstance(value, datetime):
            returnStruct = f"{key}:timestamp,"
            serializable_val = ts_ser(value)
        elif isinstance(value, date):
            returnStruct = f"{key}:date,"
            serializable_val = value.isoformat()
        elif 'NoneType' in str(type(value)):
            raise Exception("Null is not acceptable as a json value in the json_to_glue_struct function.")
        elif 'dict' in str(type(value)):
            objectStruct, serializable_val = working_with_objects_json_body(key, value, jsonObjectProp[key])
        elif 'list' in str(type(value)):
            objectStruct, serializable_val = working_with_arrays_json_body(key, value, jsonObjectProp[key])
        else:
            serializable_val = ""
        serializable_values_d[key] = serializable_val

        returnStruct += objectStruct
    returnStruct = returnStruct[:-1] + ">,"

    return returnStruct, serializable_values_d


def working_with_arrays_json_body(key, list_value: str, jsonSchemaLoadProp: dict) -> Tuple[str, Any]:
    """
    This function works to provide a return in the glue structure format when the data type is specified as array, further allowing the inclusion of objects in the array and other arrays within it.
    """
    returnArrayStruct = f"{key}:array<"

    if '{' in str(jsonSchemaLoadProp) and '}' in str(jsonSchemaLoadProp):
        returnStruct, serializable_values_d = working_with_objects_json_body(key, list_value, jsonSchemaLoadProp, True)
        returnArrayStruct += returnStruct
    else:
        serializable_values_d = []
        for value in list_value:
            if 'int' in str(type(value)):
                arrayStruct = f"int,"
                serializable_value = value
            elif 'float' in str(type(value)):
                arrayStruct = f"double,"
                serializable_value = value
            elif 'str' in str(type(value)):
                arrayStruct = f"string,"
                serializable_value = value
            elif 'bool' in str(type(value)):
                arrayStruct = f"boolean,"
                serializable_value = value
            elif isinstance(value, datetime):
                arrayStruct = f"{key}:timestamp,"
                serializable_value = ts_ser(value)
            elif isinstance(value, date):
                arrayStruct = f"{key}:date,"
                serializable_value = value.isoformat()
            elif 'NoneType' in str(type(value)):
                raise Exception("Null is not acceptable as a json value in the json_to_glue_struct function.")
            elif 'list' in str(type(value)):
                arrayStruct = working_with_arrays_json_body(key, value, value)
            else:
                pass

            returnArrayStruct += arrayStruct
            serializable_values_d.append(serializable_value)

    returnArrayStruct = returnArrayStruct[:-1] + ">,"
    return returnArrayStruct, serializable_values_d


def ts_ser(value: datetime)-> str:
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def working_with_types_json_body(key: str, value: str, jsonBodyLoadProp: dict) -> Tuple[str, Any]:
    """
    This function performs the basic handling for simple data types like number, string, integer... And for more complex types like array and object it asks for the help of another function.
    """
    returnStruct = ""
    if 'int' in str(type(value)):
        returnStruct = f"{key}:int,"
        serializable_val = value
    elif 'float' in str(type(value)):
        returnStruct = f"{key}:double,"
        serializable_val = value
    elif 'str' in str(type(value)):
        returnStruct = f"{key}:string,"
        serializable_val = value
    elif 'bool' in str(type(value)):
        returnStruct = f"{key}:boolean,"
        serializable_val = value
    elif isinstance(value, date):
        returnStruct = f"{key}:date,"
        serializable_val = value.isoformat()
    elif 'NoneType' in str(type(value)):
        raise Exception("Null is not acceptable as a json value in the json_to_glue_struct function.")
    elif 'dict' in str(type(value)):
        returnStruct, serializable_val = working_with_objects_json_body(key, value, jsonBodyLoadProp[key])
    elif 'list' in str(type(value)):
        returnStruct, serializable_val = working_with_arrays_json_body(key, value, jsonBodyLoadProp[key])
    else:
        pass

    return returnStruct, serializable_val


def struct_validator(glueStructString: str) -> bool:
    """
    This function works to validate the glue struct string generated by the main functions. But it can be used to check on demand glue struct strings too.
    """
    strict_inequality_count = glueStructString.count('<') + glueStructString.count('>')

    if (strict_inequality_count % 2) != 0:
        raise Exception('The count of strict inequality is not Even.')
    elif "<>" in glueStructString or "< >" in glueStructString or "><" in glueStructString or "> <" in glueStructString:
        raise Exception('The strict inequality is empty or malformed.')
    else:
        return True
