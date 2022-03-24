from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import LoadJob
from google.cloud.bigquery.dataset import Dataset, DatasetReference
from google.cloud.bigquery.table import TableReference
from google.cloud.exceptions import NotFound
from typing import List, TypeVar
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
    table_builder.prepare_jobs()

    for table_id, uri in table_builder.uri_paths_dict.items():
        table_id: str
        uri: str
        job: LoadJob = client.load_table_from_uri(source_uris=uri, destination=TableReference(dt.reference, table_id),
                                                  location=location, job_config=table_builder.jobs[table_id])
        job.result()

