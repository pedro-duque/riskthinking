import polars as pl
#from polars.internals import DataFrame
import os
from deltalake import DeltaTable
class PolarsReadRaw:

    def __init__(self, path: str):
        self.path = path

    def MultipleCsvFiles(self):

        return pl.scan_csv(self.path, infer_schema_length=0).collect()

    def ReadCsvFileFileName(self, file_path: str):
        print(file_path)
        filename = os.path.basename(file_path)
        df = pl.read_csv(file_path, infer_schema_length=0)
        df = df.with_columns(File_name = pl.lit(filename))
        
        return df

    def MultipleCsvFilesFileName(self, file_paths: list, num_processes: int):
        from multiprocessing import Pool
        # multiprocessing pool
        
        with Pool(processes=num_processes) as pool:
            # Use to read files in parallel
            dfs = pool.map(self.ReadCsvFileFileName, file_paths)
        
        return pl.concat(dfs)

class DeltaReadTable:

    def __init__(self, path: str):
        self.path = path

    def DeltaFormat(self):

        dt = DeltaTable(self.path)

        return dt

    def PandasFormat(self):

        dt = DeltaTable(self.path)

        return dt.to_pandas()