import inspect
import importlib
from typing import Union, TypeVar, List
from .pipelines import BasePipeline
from .bqschemabuilder import BaseBuilder

T = TypeVar('T')


def find_pipeline_class(pipeline: Union[None, str], module: str) -> T:
    if pipeline is None and module == 'vizproc.pipelines':

        return BasePipeline

    elif pipeline is None and module == 'vizproc.bqschemabuilder':

        return BaseBuilder

    for _, cls in inspect.getmembers(importlib.import_module(module), inspect.isclass):
        if cls.__dict__.get('NAME') == pipeline.lower():

            return cls

    if module == 'vizproc.pipelines':

        return BasePipeline

    elif module == 'vizproc.bqschemabuilder':

        return BaseBuilder


def exceltocsv(file: str, dest: str, schema_folder: str, pipeline: Union[None, str]) -> Union[str, List[str]]:
    cls: T = find_pipeline_class(pipeline, 'vizproc.pipelines')
    dest_files: Union[str, List[str]] = cls(file, dest, schema_folder).save()

    return dest_files

