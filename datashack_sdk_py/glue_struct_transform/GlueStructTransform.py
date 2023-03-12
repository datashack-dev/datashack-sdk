import json
from typing import Set, Tuple

from datashack_sdk_py.glue_struct_transform.utils import working_with_types_json_body, struct_validator, \
    working_with_types


class GlueStructTransform:
    """
    Instantiate a GlueStructTransform operation.
    
    This is the glue transform class, you  can call the function that converts:
    - Json Schema > Glue Struct
    """

    def __init__(self):
        pass

    def json_schema_to_glue_struct(jsonSchemaLoadProp: dict, *args, **kwargs) -> str:
        """
        This function performs a loop for each data inside the json schema to understand the data type and return a string in the glue structure format at the end.
        """

        objectField = kwargs.get('objectField', None)
        fullSchema = kwargs.get('fullSchema', True)

        if fullSchema == False:
            loopJsonSchemaLoadProp = jsonSchemaLoadProp['properties'][f'{objectField}']['properties']
        elif fullSchema == True:
            loopJsonSchemaLoadProp = jsonSchemaLoadProp['properties']

        tempStruct = ""
        for item in loopJsonSchemaLoadProp:
            result = working_with_types(item, loopJsonSchemaLoadProp)
            tempStruct += result

        if fullSchema == False:
            finalGlueStruct = f"struct<{tempStruct[:-1]}>"
        elif fullSchema == True:
            finalGlueStruct = tempStruct[:-1]

        glue_schema_string_validation = struct_validator(finalGlueStruct)

        if glue_schema_string_validation is True:
            return finalGlueStruct

    @classmethod
    def datashack_json_to_glue_struct(cls, jsonBody: dict, **kwargs) -> Tuple[Set, str]:
        """
        This function performs a loop for each data inside the json to understand the data type and return a string in the glue structure format at the end.
        """

        objectField = kwargs.get('objectField', None)
        fullBody = kwargs.get('fullBody', True)

        if not fullBody:
            loop_json_body_load_prop = jsonBody[f'{objectField}']
        else:
            loop_json_body_load_prop = jsonBody

        temp_struct = set()
        temp_serializable_data = {}

        for key, value in loop_json_body_load_prop.items():
            result,serializable_value = working_with_types_json_body(key, value, loop_json_body_load_prop)
            result = result[:-1] if result[-1] == "," else result
            temp_struct.add(result)
            temp_serializable_data[key] = serializable_value
        return temp_struct, json.dumps(temp_serializable_data)
        # if fullBody == False:
        #     finalGlueStruct = f"struct<{temp_struct[:-1]}>"
        # elif fullBody == True:
        #     finalGlueStruct =  temp_struct[:-1]
        #
        # glue_schema_string_validation = struct_validator(finalGlueStruct)
        #
        # if glue_schema_string_validation is True:
        #     return finalGlueStruct
