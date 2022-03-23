from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames
from typing import Dict, List, Union
from json import load


def convert_types(np_type: str) -> Dict[str, Union[SqlTypeNames, int]]:
    if np_type.count('O') > 0 or np_type.count('S') > 0 or np_type.count('U') > 0:

        return {'field_type': SqlTypeNames.STRING}

    elif np_type.count('i') > 0:

        return {'field_type': SqlTypeNames.INTEGER}

    elif np_type.count('f') > 0:

        return {'field_type': SqlTypeNames.NUMERIC, 'precision': 14, 'scale': 2}

    elif np_type.count('M') > 0:

        return {'field_type': SqlTypeNames.DATETIME}


def build_schema(fields: Dict[str, str]) -> List[SchemaField]:
    schema = []
    for key, value in fields.items():
        key: str
        value: str
        params: Dict[str, Union[SqlTypeNames, int]] = convert_types(value)
        schema.append(SchemaField(name=key.lower(), **params))

    return schema


if __name__ == '__main__':

    with open('../../datasets/ncaa/schema/finances_fact.json', 'r') as f:
        d = load(f)

    print(d)
    print(build_schema(d))