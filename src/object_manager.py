import os

class ObjectManager:
    def __init__(self, storage_path):
        self.storage_path = storage_path

    def putObject(self, bucket_name, key, chunks):

        object_path = os.path.join(self.storage_path, bucket_name, key)

        os.makedirs(object_path, exist_ok=True)

        #versioning

        version = self._get_next_version(object_path)

        version_path = os.path.join(object_path, version)
        os.makedirs(version_path, exist_ok=True)
        for i, chunk in enumerate(chunks):
            chunk_path = os.path.join(version_path, f"chunk_{i+1}")
            with open(chunk_path, 'wb') as f:
                f.write(chunk)

        print(f"Object {key} stored successfully in bucket {bucket_name} with version {version}.")
        return True

    def _get_next_version(self, object_path):
        if not os.path.exists(object_path):
            return "v1"

        versions = [d for d in os.listdir(object_path) if d.startswith('v')]
        if not versions:
            return "v1"

        latest_version = max(int(v[1:]) for v in versions)
        return f"v{latest_version + 1}"

