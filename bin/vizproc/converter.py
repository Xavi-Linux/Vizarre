import pandas as pd
from typing import Dict, Union, List
from pipelines import NCAA, BasePipeline


def exceltocsv(filepath: str, dest: str) -> None:
    NCAA(filepath, dest).save()


if __name__ == '__main__':
    dest_folder: str = '../'
    path: str = '../../datasets/unprocessed/NCAA_PL.xlsx'
    exceltocsv(path, dest_folder)