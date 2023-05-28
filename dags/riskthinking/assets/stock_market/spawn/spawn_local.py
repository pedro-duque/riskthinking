from deltalake.writer import write_deltalake
#from polars.internals import DataFrame
import polars as pl
import pyarrow as pa
import os

class DeltaWrite:

    def __init__(self, path: str, pl_dataframe):
        self.path = path
        self.pd_dataframe = pl_dataframe.to_pandas(use_pyarrow_extension_array=True)

    def WithoutPartitionOverwrite(self):

        write_deltalake(self.path, self.pd_dataframe, mode='overwrite', overwrite_schema=True)

        return {"Path": self.path,"status": True}

    def WithoutPartitionAppend(self):

        write_deltalake(self.path, self.pd_dataframe, mode='append', overwrite_schema=True)

        return {"status": True}

    #def PartitionOverwrite(self):

    #def PartitionAppend(seld):

class ModelWrite:

    def __init__(self, model, metrics):
        self.model = model
        self.metrics = metrics
    
    def DumpJoblib(self, path, filename):
        import joblib
        if not os.path.exists(path):
            os.makedirs(path)
        joblib.dump(self.model, path+"/"+filename)

        return {"Path": path+"/"+filename, "status": True}

    def DumpPickle(self, path, filename):
        import pickle
        if not os.path.exists(path):
            os.makedirs(path)
        pickle.dump(self.model, open(path+"/"+filename,'wb'))

        return {"Path": path+"/"+filename, "status": True}


    def TrainingMetrics(self, path, filename):
        import json
        
        jsonString = json.dumps(self.metrics)
        jsonFile = open(path+"/"+filename, "w")
        jsonFile.write(jsonString)
        jsonFile.close()

        return {"Path": path+"/"+filename, "status": True}


