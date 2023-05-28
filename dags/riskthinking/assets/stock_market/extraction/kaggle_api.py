# Class to handle Kaggle's dataset metadata and download it
import kaggle
class KaggleDataset:
    
    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name

    def Metadata(self) -> dict:
        
        return {"Metadata": vars(kaggle.api.dataset_view(self.dataset_name))}

    def Download(self, path: str) -> dict:
        from os import listdir

        kaggle.api.dataset_download_files(self.dataset_name, path=path, force=True, unzip=True)

        return {"path": path, "list_dir": listdir(path)}