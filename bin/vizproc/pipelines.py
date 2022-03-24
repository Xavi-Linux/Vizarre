import pandas as pd
from typing import Union, Dict, List, Callable
import numpy as np
from json import dump
from io import StringIO


class BasePipeline:

    NAME: str = 'base'
    EXT: str = '.csv'

    def __init__(self, origin: str, dest: str, schema: str):
        self.origin_path: str = origin
        self.dest_path: str = dest
        self.schema_path: str = schema
        self.data: Union[None, pd.DataFrame, dict[pd.DataFrame]] = None
        self.transformed: bool = False
        self.schema_saved: bool = False

    @staticmethod
    def _first_step(func: Callable[['BasePipeline'], None]) -> Callable[['BasePipeline'], None]:
        def wrapper(self: 'BasePipeline'):
            if self.data is None:
                self.read()

            return func(self)

        return wrapper

    @staticmethod
    def _second_step(func: Callable[['BasePipeline'], None]) -> Callable[['BasePipeline'], None]:
        def wrapper(self: 'BasePipeline'):
            if not self.transformed:
                self.transform()

            return func(self)

        return wrapper

    @staticmethod
    def _third_step(func: Callable[['BasePipeline'], None]) -> Callable[['BasePipeline'], None]:
        def wrapper(self: 'BasePipeline'):
            if not self.schema_saved:
                self.save_schema()

            return func(self)

        return wrapper

    def __dump(self, df: pd.DataFrame, name: str) -> None:
        columns: Dict = dict(map(lambda k: (k[0], k[1].str),
                                 df.dtypes.to_dict().items()
                                 )
                             )
        with open(self.schema_path + name + '.json', 'w') as f:
            f:StringIO
            dump(columns, f)

    def read(self) -> None:
        self.data = pd.read_excel(self.origin_path, sheet_name=None, engine='openpyxl')

    @_first_step.__func__
    def transform(self) -> None:
        self.transformed = True

    @_second_step.__func__
    @_first_step.__func__
    def save_schema(self) -> None:
        self.schema_saved = True
        if isinstance(self.data, pd.DataFrame):
            self.__dump(self.data, self.NAME)

        elif isinstance(self.data, Dict):
            for key, value in self.data.items():
                key:str
                value: pd.DataFrame
                self.__dump(value, key)

    @_third_step.__func__
    @_second_step.__func__
    @_first_step.__func__
    def save(self) -> Union[str, List[str]]:
        if isinstance(self.data, pd.DataFrame):
            self.data.to_csv(path_or_buf=self.dest_path + self.NAME + self.EXT, index=False)

            return self.dest_path + self.NAME + self.EXT

        elif isinstance(self.data, Dict):
            paths: List[str] = []
            for key, value in self.data.items():
                key:str
                value: pd.DataFrame
                value.to_csv(path_or_buf=self.dest_path + key + self.EXT, index=False)
                paths.append(self.dest_path + key + self.EXT)

            return paths


class NCAA(BasePipeline):

    NAME: str = 'ncaa'
    ID_FIELD: str = 'IPEDS_ID'
    CONFERENCE_FIELD: str = 'FBS_Conference'
    YEAR: str = 'Year'

    def __transform_columns(self) -> None:
        for key, value in self.data.items():
            key: str
            value: pd.DataFrame
            self.data[key] = value.rename(columns=lambda c: c.replace(' ', '_')
                                                             .replace(',', '_')
                                                             .replace('/', '_')
                                                             .replace('(', '_')
                                                             .replace(')', '_')
                                                             .replace('-', '_')
                                                             .replace(' ',''))

    def transform(self) -> None:
        super(NCAA, self).transform()
        self.__transform_columns()

        mask = self.data['finances_fact'][self.ID_FIELD].notna() & \
               self.data['finances_fact'][self.CONFERENCE_FIELD].notna()
        self.data['finances_fact'] = self.data['finances_fact'][mask]
        self.data['finances_fact'][self.data['finances_fact'].columns[4:]] = \
            self.data['finances_fact'][self.data['finances_fact'].columns[4:]].fillna(value=0)
        self.data['finances_fact'][self.ID_FIELD] = self.data['finances_fact'][self.ID_FIELD].astype(np.int32)
        self.data['finances_fact'][self.YEAR] = self.data['finances_fact'][self.YEAR].astype(np.int32)

        self.data['school_dim'][self.ID_FIELD] = self.data['school_dim'][self.ID_FIELD].astype(np.int32)



