import os
import json
import hashlib

class MetaDataManager():
    def __init__(self, base_path: str):
        self.base_path=base_path
        self.store_meta_file = os.path.join(base_path, "store_meta_data.json")

        if not os.path.exists(self.store_meta_file):
            self._write_json(self.store_meta_file, {"buckets": []})      
    
    def _read_json(self,path):
        """
       reads and returns the contents of the json files.
        
        Args:path (str): The file path where the JSON file is saved.
        
        Returns: contents of json
        """
        if not os.path.exists(path):
            return {}
        with open(path,"r") as f:
            return json.load(f)
        
    def _write_json(self, path, data):
        """
    Writes the given data to a JSON file.

    Args:
        path (str): The file path where the JSON file will be saved.
        data (dict or list): The data to be written into the JSON file.

    Returns:
        None
    """
        with open(path, "w") as f:
            json.dump(data, f, indent=4)   

    def load_store_metadata(self) :
        """
        Load the contents of store meta data.
        Returns:
            dict: Store metadata containing bucket information.
              Example: {"buckets": []}
        
        """
        path = os.path.join(self.base_path, "store_meta_data.json")
        return self._read_json(path) or {"buckets": []}

    def save_store_metadata(self, bucket_name) :
        """
        Saves the bucket name to the store metadata file if it does not already exist.

        Args:
        bucket_name (str): Name of the bucket to be added.

        Returns:
            None
        """
        path = os.path.join(self.base_path, "store_meta_data.json")
        data = self._read_json(path)
        if bucket_name not in data["buckets"]:
            data['buckets'].append(bucket_name)
            self._write_json(path, data)
    
    def load_bucket_metadata(self, bucket):
        """
        Loads metadata for a specific bucket.

        Args:
            bucket (str): Name of the bucket whose metadata is to be loaded.

        Returns:
            dict: Bucket metadata containing stored objects.
                Example: {"objects": {}}

        """
        path = os.path.join(self.base_path, bucket, "bucket_meta_data.json")
        return self._read_json(path) or {"objects": {}}

    def save_bucket_metadata(self, bucket, data=None):
        """
        Saves metadata for a specific bucket.

        Args:
            bucket (str): Name of the bucket whose metadata is to be saved.
            data (dict, optional): Metadata to store. Defaults to {"objects": {}}.

        Returns:
            None
        """
        if data is None:
            data = {"objects": {}}
        bucket_path = os.path.join(self.base_path, bucket)
        path = os.path.join(bucket_path, "bucket_meta_data.json")
        # Always write (overwrite old metadata)
        self._write_json(path, data)

    def load_object_metadata(self, bucket, key):
        """
        Loads metadata for a specific object within a bucket.

        Args:
            bucket (str): Name of the bucket containing the object.
            key (str): Identifier (path/name) of the object.

        Returns:
            dict: Object metadata including version information.
                Example: {"key": key, "versions": {}}
        """
        path = os.path.join(self.base_path, bucket, key, "object_meta_data.json")
        return self._read_json(path) or {"key": key, "versions": {}}

    def save_object_metadata(self, bucket, key, data: dict):
        """
        Saves metadata for a specific object within a bucket.

        Args:
            bucket (str): Name of the bucket containing the object.
            key (str): Identifier (path/name) of the object.
            data (dict): Metadata to be stored for the object.

        Returns:
            None

        """
        object_path = os.path.join(self.base_path, bucket, key)
        os.makedirs(object_path, exist_ok=True)

        path = os.path.join(object_path, "object_meta_data.json")
        self._write_json(path, data)



        