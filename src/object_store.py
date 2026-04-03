import os

class ObjectStore:
    def __init__(self, storage_path = "data"):
        self.storage_path = storage_path
        os.makedirs(self.storage_path, exist_ok=True)