import pandas as pd
from typing import Union, Dict, List
import numpy as np


class BasePipeline:

    NAME: str = 'base'
    EXT: str = '.csv'

    def __init__(self, origin: str, dest: str):
        self.origin_path: str = origin
        self.dest_path: str = dest
        self.data: Union[None, pd.DataFrame, dict[pd.DataFrame]] = None

    def read(self) -> None:
        self.data = pd.read_excel(self.origin_path, sheet_name=None, engine='openpyxl')

    def transform(self) -> None:
        if self.data is None:
            self.read()

    def save(self) -> Union[str, List[str]]:
        if self.data is None:
            self.read()
            self.transform()

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
            self.data[key] = value.rename(columns=lambda c: c.replace(' ', '_'))

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



