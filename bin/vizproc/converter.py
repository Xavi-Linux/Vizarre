import pandas as pd
import inspect
import importlib
from typing import Union, TypeVar, List
from .pipelines import BasePipeline

T = TypeVar('T')


def find_pipeline_class(pipeline: Union[None, str]) -> T:
    if pipeline is None:

        return BasePipeline

    for _, cls in inspect.getmembers(importlib.import_module('vizproc.pipelines'), inspect.isclass):
        if cls.__dict__['NAME'] == pipeline.lower():

            return cls

    return BasePipeline


def exceltocsv(file: str, dest: str, pipeline: Union[None, str]) -> Union[str, List[str]]:
    cls: T = find_pipeline_class(pipeline)
    dest_files: Union[str, List[str]] = cls(file, dest).save()

    return dest_files

