import os
from src.meta_data_manager import MetaDataManager
from src.chunk_manager import ChunkManager
class ObjectStore:
    def __init__(self, base_path = "object-store", chunk_size = 1024):
        self.base_path = base_path
        self.chunk_size = chunk_size
        
        os.makedirs(self.base_path, exist_ok=True)
        self.metadata_manager=MetaDataManager()
        self.chunk_manager=ChunkManager(chunk_size)

    def createBucket(self, bucket_name: str) -> bool:
        bucket_path = os.path.join(self.base_path, bucket_name)
        
        if os.path.exists(bucket_path):
            print("Bucket already exists.")
            return False

        os.makedirs(bucket_path)
        self.metadata_manager.save_store_metadata(bucket_name)
        self.metadata_manager.save_bucket_metadata(bucket_name)
        print(f"Bucket {bucket_name} created successfully.")
        return True

    def bucketExists(self, bucket_name: str) -> bool:
        bucket_path = os.path.join(self.base_path, bucket_name)
        return os.path.exists(bucket_path)
    
    def objectExists(self, bucket_name: str, key: str) -> bool:
        object_path = os.path.join(self.base_path, bucket_name, key)
        return os.path.exists(object_path)


    def putObject(self, bucket_name: str, key: str, data_path: str) -> bool:

        if not self.bucketExists(bucket_name): 
            print(f"Bucket {bucket_name} does not exist") 
            return False 
        if not os.path.exists(data_path): 
            print(f"{data_path} does not exist.")
            return False

        object_path = os.path.join(self.base_path, bucket_name, key)
        os.makedirs(object_path, exist_ok=True)

        with open(data_path, 'rb') as f: 
            object_data = f.read()

        file_hash = self.chunk_manager.compute_hash(object_data)

        version = self.getNextVersion(object_path)
        version_path = os.path.join(object_path, version)
        os.makedirs(version_path, exist_ok=True)

        chunks = self.chunk_manager.split_into_chunks(object_data) 
        self.chunk_manager.write_chunks(version_path,chunks)

        obj_meta = self.metadata_manager.load_object_metadata(bucket_name, key)

        obj_meta["versions"][version] = {
            "chunkCount": len(chunks),
            "hash": file_hash
        }

        self.metadata_manager.save_object_metadata(bucket_name, key, obj_meta)

        bucket_meta = self.metadata_manager.load_bucket_metadata(bucket_name)
        bucket_meta["objects"][key] = version
        self.metadata_manager.save_bucket_metadata(bucket_name, bucket_meta)

        print(f"Stored {key} in {bucket_name}/{version}")
        return True

    def getNextVersion(self, object_path) -> str:
        if not os.path.exists(object_path):
            return "v1"

        versions = [d for d in os.listdir(object_path) if d.startswith('v')]
        if not versions:
            return "v1"

        latest_version = max(int(v[1:]) for v in versions)
        return f"v{latest_version + 1}"


    def getObject(self, bucket_name, key, version=None)-> bool:

        if not self.bucketExists(bucket_name):
            print(f"Bucket {bucket_name} does not exist") 
            return False
        
        if not self.objectExists(bucket_name, key):
            print(f"{key} does not exist") 
            return False

        object_path = os.path.join(self.base_path, bucket_name, key)

        # Load metadata
        obj_meta = self.metadata_manager.load_object_metadata(bucket_name, key)

        if not obj_meta["versions"]:
            print("No versions found.")
            return False

        # Get latest version if not provided
        if version is None:
            version = max(obj_meta["versions"].keys(), key=lambda v: int(v[1:]))

        # Validate version
        if version not in obj_meta["versions"]:
            print(f"Version {version} not found.")
            return False

        version_info = obj_meta["versions"][version]
        chunk_count = version_info["chunkCount"]
        expected_hash = version_info["hash"]

        version_path = os.path.join(object_path, version)
        filename = os.path.basename(key)

        reconstructed_data = self.chunk_manager.read_chunks(version_path, chunk_count)

        # Verify integrity
        actual_hash = self.chunk_manager.compute_hash(reconstructed_data)

        if actual_hash != expected_hash:
            print("WARNING: Data corruption detected!")
            return False
        else:
            print("No corruption detected.")

        # Save output
        with open(f"reconstructed_{filename}", "wb") as out:
            out.write(reconstructed_data)

        print(f"Retrieved {key}({version}) as reconstructed_{filename}")

        return True
        
    def getLatestVersion(self, object_path):

        versions = [d for d in os.listdir(object_path) if d.startswith("v")]

        if not versions:
            return None

        latest = max(int(v[1:]) for v in versions)
        return f"v{latest}"

    def listObjects(self, bucket_name, prefix=None) -> None:

        # Check bucket existence
        store_meta = self.metadata_manager.load_store_metadata()

        if bucket_name not in store_meta.get("buckets", []):
            print("Bucket does not exist.")
            return

        # Load bucket metadata
        bucket_meta = self.metadata_manager.load_bucket_metadata(bucket_name)
        objects = bucket_meta.get("objects", {})

        # Apply prefix filter
        if prefix:
            objects = {
                key: version
                for key, version in objects.items()
                if key.startswith(prefix)
            }

        print(f"{bucket_name}")
        for key, version in objects.items():
            print(f"| {key} -> {version}")
