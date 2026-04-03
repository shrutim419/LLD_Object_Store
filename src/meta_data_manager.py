import os
import json
import hashlib

class MetaDataManager():
    def __init__(self, base_path="object-store"):
        self.base_path=base_path
        self.store_meta_file = os.path.join(base_path, "store_meta_data.json")

        if not os.path.exists(self.store_meta_file):
            self._write_json(self.store_meta_file, {"buckets": []})      
    
    def _read_json(self,path):
        if not os.path.exists(path):
            return {}
        with open(path,"r") as f:
            return json.load(f)
        
    def _write_json(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f, indent=4)   


    def load_store_metadata(self) :
        path = os.path.join(self.base_path, "store_meta_data.json")
        return self._read_json(path) or {"buckets": []}

    def save_store_metadata(self, bucket_name) :
        path = os.path.join(self.base_path, "store_meta_data.json")
        data = self._read_json(path)
        if bucket_name not in data["buckets"]:
            data['buckets'].append(bucket_name)
            data= self._write_json(path, data)
    
    def load_bucket_metadata(self, bucket):
        path = os.path.join(self.base_path, bucket, "bucket_meta_data.json")
        return self._read_json(path) or {"objects": {}}

    def save_bucket_metadata(self, bucket, data=None):
        if data is None:
            data = {"objects": {}}
        bucket_path = os.path.join(self.base_path, bucket)

        path = os.path.join(bucket_path, "bucket_meta_data.json")
        # Always write (overwrite old metadata)
        self._write_json(path, data)

    def load_object_metadata(self, bucket, key):
        path = os.path.join(self.base_path, bucket, key, "object_meta_data.json")
        return self._read_json(path) or {"key": key, "versions": {}}

    def save_object_metadata(self, bucket, key, data: dict):
        object_path = os.path.join(self.base_path, bucket, key)
        os.makedirs(object_path, exist_ok=True)

        path = os.path.join(object_path, "object_meta_data.json")
        self._write_json(path, data)



        