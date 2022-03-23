from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import LoadJob, LoadJobConfig
from google.cloud.bigquery.dataset import Dataset, DatasetReference
from google.cloud.bigquery.table import Table
from google.cloud.exceptions import NotFound
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.enums import SourceFormat
from typing import List, TypeVar, Dict
from .converter import find_pipeline_class

T = TypeVar('T')


def delete_dataset(project: str, dataset: str) -> None:
    client = Client(project)

    client.delete_dataset(dataset,
                          delete_contents=True)


def commit_tables(project_id: str, dataset:str, location: str, schema_path: str, tables: List[str]) -> None:
    client: Client = Client(project_id)
    dataset_ref: DatasetReference = DatasetReference(project=project_id, dataset_id=dataset)
    try:
        dt: Dataset = client.get_dataset(dataset_ref=dataset_ref)

    except NotFound:
        dt: Dataset = Dataset(dataset_ref)
        dt.location = location
        dt: Dataset = client.create_dataset(dt, exists_ok=True)

    cls: T = find_pipeline_class(dataset, 'vizproc.bqschemabuilder')

    table_builder: T = cls(tables=tables, schema_path=schema_path, dataset=dt)
    _: Dict[str,List[SchemaField]] = table_builder.retrieve_schemas()
    bq_tables: Dict[str, Table] = table_builder.retrieve_tables()

    for uri, key in zip(tables, bq_tables):
        uri: str
        key: str
        bq_tables[key]._properties['location'] = location
        job_config: LoadJobConfig = LoadJobConfig(skip_leading_rows=1, field_delimiter=',',
                                                  source_format=SourceFormat.CSV,
                                                  schema=table_builder.schemas[key],
                                                  clustering_fields=bq_tables[key].clustering_fields)
        job: LoadJob = client.load_table_from_uri(source_uris=uri, destination=bq_tables[key],
                                                  location=location, job_config=job_config)
        job.result()

