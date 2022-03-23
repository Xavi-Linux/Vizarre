from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.table import Table, TableReference
from google.cloud.bigquery.dataset import Dataset
from typing import Dict, List, Union
from json import load
from pathlib import Path
from io import StringIO


def convert_types(np_type: str) -> Dict[str, Union[SqlTypeNames, int]]:
    if np_type.count('O') > 0 or np_type.count('S') > 0 or np_type.count('U') > 0:

        return {'field_type': SqlTypeNames.STRING}

    elif np_type.count('i') > 0:

        return {'field_type': SqlTypeNames.INTEGER}

    elif np_type.count('f') > 0:

        return {'field_type': SqlTypeNames.FLOAT64}

    elif np_type.count('M') > 0:

        return {'field_type': SqlTypeNames.DATETIME}


def build_schema(fields: Dict[str, str]) -> List[SchemaField]:
    schema = []
    for key, value in fields.items():
        key: str
        value: str
        params: Dict[str, Union[SqlTypeNames, int]] = convert_types(value)
        schema.append(SchemaField(name=key.lower().replace('.', '_'), **params))

    return schema


class BaseBuilder:

    NAME: str = 'base'
    EXT: str = '.json'

    def __init__(self, tables:List[str], schema_path:str, dataset: Dataset):
        self.uri_paths: List[str] = tables
        self.schema_paths: List[str] = list(map(lambda p: schema_path + '/' + Path(p).stem + self.EXT,
                                                self.uri_paths))
        self.table_ids: List[str] = list(map(lambda p: Path(p).stem,
                                             self.uri_paths))
        self.schemas: Dict[str, Union[None, List[SchemaField]]] = dict(zip(self.table_ids, [None] * len(self.table_ids)))
        self.dataset: Dataset = dataset

    def retrieve_schemas(self) -> Dict[str,List[SchemaField]]:
        for key, path in zip(self.table_ids, self.schema_paths):
            key: str
            path: str
            with open(path, 'r') as f:
                f: StringIO
                fields: Dict[str, str] = load(f)

            self.schemas[key] = build_schema(fields)

        return self.schemas

    def retrieve_tables(self) -> Dict[str, Table]:
        bq_tables: Dict[str, Table] = {}
        for key, value in self.schemas.items():
            key: str
            value: List[SchemaField]
            bq_tables[key] = Table(table_ref=TableReference(self.dataset.reference, key), schema=value)

        return bq_tables


class NCAABuilder(BaseBuilder):

    NAME: str = 'ncaa'

    def retrieve_schemas(self) -> Dict[str,List[SchemaField]]:
        _: Dict[str,List[SchemaField]] = super(NCAABuilder, self).retrieve_schemas()
        for schema_field in self.schemas['finances_fact']:
            schema_field: SchemaField
            if schema_field.name == 'ipeds_id':
                schema_field._properties['mode'] = 'REQUIRED'

            elif schema_field == 'year':
                schema_field._properties['mode'] = 'REQUIRED'

        for schema_field in self.schemas['school_dim']:
            schema_field: SchemaField
            schema_field._properties['mode'] = 'REQUIRED'

        for schema_field in self.schemas['conference_dim']:
            schema_field: SchemaField
            schema_field._properties['mode'] = 'REQUIRED'

        return self.schemas

    def retrieve_tables(self) -> Dict[str, Table]:
        bq_tables: Dict[str, Table] = super(NCAABuilder, self).retrieve_tables()

        bq_tables['finances_fact'].clustering_fields = ['ipeds_id', 'year']
        bq_tables['school_dim'].clustering_fields = ['ipeds_id']

        return bq_tables
