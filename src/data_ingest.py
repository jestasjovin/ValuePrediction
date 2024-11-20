import os
import zipfile
from abc import ABC, abstractmethod
import pandas as pd


#  Abstract class to ingest data
class IngestData(ABC):
    @abstractmethod
    def ingest(self, file_path: str) -> pd.DataFrame:
        pass

