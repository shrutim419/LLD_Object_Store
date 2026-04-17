import os
import shutil
from pathlib import Path
from src.meta_data_manager import MetaDataManager
from src.chunk_manager import ChunkManager
class ObjectStore:
    """
    This is a simplified object store inspired by services like Amazon S3.
    It has:
    Object storing inside buckets.
    Supports Object versioning.
    Retrieval of the latest object or specified version of object.
    Supports prefix listing of keys.
    Objects are stored on a local files system where:
    The object store has a base directory.
    Each bucket is represented as a directory inside the base directory.
    Each object has a key that is like a path to the object in the file system
    If the same object is put into a bucket a new version of the object is created.
    Object data is stored in fixed size chunks.
    Hash of the data is stored for validation.
    Meta data is stored at Object Store level, Bucket level and Object level.

    """
    
    def __init__(self, base_path: str | None = None, chunk_size : int | None = None):
        
        self.base_path = base_path or str(Path(__file__).resolve().parent.parent / "object-store")
        self.chunk_size = chunk_size or 1024

        os.makedirs(self.base_path, exist_ok=True)
        self.metadata_manager=MetaDataManager(self.base_path)
        self.chunk_manager=ChunkManager(self.chunk_size)

    def createBucket(self, bucket_name: str) -> bool:
        """Create a new bucket for storing objects.
        Parameters 
        bucket_name: Name of the bucket to create. Must be unique.
        
        Returns
            True if bucket created successfully, False if it already exists.

        """
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
        """Check if a bucket exists in the object store.
        
        Parameters:
            bucket_name: Name of the bucket to check.
        
        Returns:
            True if bucket exists. False otherwise.
        """
        data = self.metadata_manager.load_store_metadata()
        return bucket_name in data["buckets"]
    
    def objectExists(self, bucket_name: str, key: str) -> bool:
        """Check if an object exists in a bucket.
        
        Returns:
            True if object exists, False otherwise.
        """
        data = self.metadata_manager.load_bucket_metadata(bucket_name)
        return key in data.get("objects", {})

    def putObject(self, bucket_name: str, key: str, data_path: str) -> bool:
        """Store a file in the object store as a new version.
        
        Args:
            bucket_name: Destination bucket name.
            key: Object identifier/path within the bucket.
            data_path: File system path to the file to store.
        
        Returns:
            True if object stored successfully. False on validation or storage errors.
            
            - Reads file from disk and chunks it based on chunk_size
            - Writes chunks to versioned directory structure
            - Computes and stores SHA-256 hash for integrity verification
            - Updates bucket and object metadata
            - Creates new version entry
 
        """

        if not self.bucketExists(bucket_name):
            print(f"Bucket {bucket_name} does not exist")
            return False
        if not os.path.exists(data_path):
            print(f"{data_path} does not exist.")
            return False

        print(f"Storing {key} in {bucket_name}.")

        object_path = os.path.join(self.base_path, bucket_name, key)
        os.makedirs(object_path, exist_ok=True)

        with open(data_path, 'rb') as f:
            object_data = f.read()

        file_hash = self.chunk_manager.compute_hash(object_data)

        version = self.getNextVersion(bucket_name, key)
        version_path = os.path.join(object_path, version)
        os.makedirs(version_path, exist_ok=True)

        try:
            # Write chunks to disk
            chunks = self.chunk_manager.split_into_chunks(object_data)
            self.chunk_manager.write_chunks(version_path, chunks)

            # Update bucket metadata first
            bucket_meta = self.metadata_manager.load_bucket_metadata(bucket_name)
            bucket_meta["objects"][key] = version
            self.metadata_manager.save_bucket_metadata(bucket_name, bucket_meta)

            # Update object metadata second
            obj_meta = self.metadata_manager.load_object_metadata(bucket_name, key)
            obj_meta["versions"][version] = {
                "chunkCount": len(chunks),
                "hash": file_hash
            }
            self.metadata_manager.save_object_metadata(bucket_name, key, obj_meta)

            print(f"Stored {key} in {bucket_name}/{version}")
            return True

        except Exception as e:
            print(f"Error storing object: {e}")
            
            if os.path.exists(version_path):
                try:
                    shutil.rmtree(version_path)
                    print(f"Cleaned up orphaned version directory: {version_path}")
                except Exception as cleanup_error:
                    print(f"Failed to cleanup: {cleanup_error}")
            return False

    def getNextVersion(self, bucket, key) -> str:
        """Determine the next available version number for an object.
        
        Args:
            bucket: Bucket name which contains the object.
            key: Object identifier/path within the bucket.
        
        Returns:
            Next version string ( 'v1','v2','v3'). Returns 'v1' for new objects.

        """
        data = self.metadata_manager.load_object_metadata(bucket, key)
        versions = data.get("versions", {})

        if not versions:
            return "v1"

        latest = max(int(v[1:]) for v in versions.keys())
        return f"v{latest + 1}"


    def getObject(self, bucket_name, key, version=None) -> bool:
        """Retrieve an object from storage and reconstruct it from chunks.
        
        Parameters:
            bucket_name: Source bucket name.
            key: Object identifier/path within the bucket.
            version: Specific version to retrieve (e.g. 'v1', 'v2'). Defaults to latest version.
        
        Returns:
            True if object retrieved and reconstructed successfully, False on any error.

        - Reads all chunks from disk for the specified version
        - Reconstructs original file from chunks in correct order
        - Saves reconstructed file as 'reconstructed_<filename>_<version>.<ext>'
        - Verifies data integrity by comparing SHA-256 hashes
            
        Validation:
            - Checks bucket exists
            - Checks object exists in bucket
            - Validates requested version exists
            - Detects data corruption via hash mismatch
        """

        if not self.bucketExists(bucket_name):
            print(f"Bucket {bucket_name} does not exist") 
            return False
        
        if not self.objectExists(bucket_name, key):
            print(f"{key} does not exist") 
            return False

        print(f"Retrieving {key} from {bucket_name}")

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
        base_name = os.path.basename(key)
        filename, extension = os.path.splitext(base_name)

        reconstructed_data = self.chunk_manager.read_chunks(version_path, chunk_count)

        # Verify integrity
        actual_hash = self.chunk_manager.compute_hash(reconstructed_data)

        if actual_hash != expected_hash:
            print("WARNING: Data corruption detected!")
            return False
        else:
            print("No corruption detected.")

        # Save output
        with open(f"reconstructed_{filename}_{version}{extension}", "wb") as out:
            out.write(reconstructed_data)

        print(f"Retrieved {key}({version}) as reconstructed_{filename}_{version}{extension}")

        return True
# 
    def getLatestVersion(self, bucket, key) -> str | None:
        """Retrieve the latest version number of an object.
        
        Args:
            bucket: Bucket name which contains the object.
            key: Object identifier/path within the bucket.
        
        Returns:
            Latest version string (e.g., 'v5'), or None if no versions exist.            
        """

        data = self.metadata_manager.load_object_metadata(bucket, key)
        versions = data.get("versions", {})

        if not versions:
            return None

        latest = max(int(v[1:]) for v in versions.keys())
        return f"v{latest}"

    def listObjects(self, bucket_name, prefix=None) -> None:
        """List all objects in a bucket with optional prefix filtering.
        
        Parameters:
            bucket_name: Name of the bucket to list objects from.
            prefix: Optional prefix filter. Only objects whose keys start with this string
                   will be listed. If None, all objects in bucket are listed.

        """

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
