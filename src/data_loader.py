import os
import zipfile
from abc import ABC, abstractmethod
import pandas as pd


# Abstract class to define the interface for data loading
class DataLoading(ABC):
    @abstractmethod
    def load(self, file_path: str) -> pd.DataFrame:
        """Abstract method to read data and return as pandas dataframe."""
        pass


# Class to handle ZIP files and return a pandas DataFrame
class DataLoadingZip(DataLoading):
    def load(self, file_path: str) -> pd.DataFrame:
        if not file_path.endswith(".zip"):
            raise ValueError(f"Provided file '{file_path}' is not a .zip file.")

        # Extract zip file
        extraction_folder = "extracted_data"
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extraction_folder)

        # Look for CSV files inside the extracted folder
        extracted_files = os.listdir(extraction_folder)
        csv_files = [f for f in extracted_files if f.endswith(".csv")]

        if len(csv_files) == 0:
            raise FileNotFoundError(f"No CSV file found in the zip archive '{file_path}'.")
        if len(csv_files) > 1:
            raise ValueError(f"Multiple CSV files found in the zip archive. Please specify which one to use.")

        # Read the CSV file into a DataFrame
        csv_file_path = os.path.join(extraction_folder, csv_files[0])
        df = pd.read_csv(csv_file_path)

        return df


# Factory class to create the appropriate data loading instance based on file type
class DataLoadingFactory:
    @staticmethod
    def get_data_loading(file_extension: str) -> DataLoading:
        if file_extension == ".zip":
            return DataLoadingZip()
        else:
            raise ValueError(f"No data loading method available for file extension: {file_extension}")


if __name__ == "__main__":
    # Specify the path to the ZIP file
    file_path = "/home/jvjestas/samples/papers/ValuePrediction/data/archive.zip"  # Update with the actual file path

    # Determine the file extension of the given file
    file_extension = os.path.splitext(file_path)[1]

    # Get the appropriate data loading method for the file extension
    data_loading = DataLoadingFactory.get_data_loading(file_extension)

    # Load data into a DataFrame
    df = data_loading.load(file_path)

    # Print the first few rows of the dataframe
    print(df.head())
