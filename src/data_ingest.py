import os
import zipfile
from abc import ABC, abstractmethod
import pandas as pd


#  Abstract class to ingest data
class IngestData(ABC):
    @abstractmethod
    def ingest(self, file_path: str) -> pd.DataFrame:
        pass

# class to handle zip files and return as pandas data frame
class IngestZip(IngestData):
    def ingest(self, file_path: str) -> pd.DataFrame:
        if not file_path.endswith(".zip"):
            raise ValueError("Provided file is not a .zip file.")

        # Extract zip file
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall("extracted_data")

        # assuming there is one CSV file inside the zip
        extracted_files = os.listdir("extracted_data")
        csv_files = [f for f in extracted_files if f.endswith(".csv")]

        if len(csv_files) == 0:
            raise FileNotFoundError("No CSV file found.")
        if len(csv_files) > 1:
            raise ValueError("Multiple CSV files found. Please specify which one to use.")

        # Read the CSV into a DataFrame
        csv_file_path = os.path.join("extracted_data", csv_files[0])
        df = pd.read_csv(csv_file_path)

        # Return the DataFrame
        return df


# Factory to create DataIngestors
# this enables having support for different file types
class DataIngestorFactory:
    @staticmethod
    def get_data_ingestor(file_extension: str) -> IngestData:
        if file_extension == ".zip":
            return IngestZip()
        else:
            raise ValueError(f"No ingestor available for file extension: {file_extension}")


if __name__ == "__main__":
    # my file path
    file_path = "/home/jvjestas/samples/price_prediction/data/archive.zip"  # Change this to your actual file path

    # Determine the file extension
    file_extension = os.path.splitext(file_path)[1]

    # appropiate data ingestor
    data_ingestor = DataIngestorFactory.get_data_ingestor(file_extension)

    # Ingest the data and load it into a DataFrame
    df = data_ingestor.ingest(file_path)

    print(df.head()) 
    pass
