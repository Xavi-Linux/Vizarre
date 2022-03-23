from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import LoadJob, LoadJobConfig
from google.cloud.bigquery.dataset import Dataset, DatasetReference
from google.cloud.exceptions import NotFound


def create_dataset(project: str, dataset: str) -> Dataset:
    client = Client(project)
    ds: Dataset = Dataset(DatasetReference(project=project, dataset_id=dataset))
    ds.location = 'europe-west1'
    ds.description = 'NCAA PnL'

    return client.create_dataset(ds, exists_ok=True)


def delete_dataset(project: str, dataset: str) -> None:
    client = Client(project)

    client.delete_dataset(dataset,
                          delete_contents=True)


if __name__ == '__main__':
    delete_dataset('xavi-linux-begins', 'ncaa')
