import os

class ObjectStore:
    def __init__(self, base_path = "object-store", chunk_size = 1024):
        self.base_path = base_path
        self.chunk_size = chunk_size
        
        os.makedirs(self.base_path, exist_ok=True)

    def createBucket(self, bucket_name):
        bucket_path = os.path.join(self.base_path, bucket_name)
        
        if os.path.exists(bucket_path):
            print("Bucket already exists.")
            return False

        os.makedirs(bucket_path)
        print(f"Bucket {bucket_name} created successfully.")
        return True

    def bucketExists(self, bucket_name):
        bucket_path = os.path.join(self.base_path, bucket_name)
        return os.path.exists(bucket_path)


    def putObject(self, bucket_name, key, chunks):

        object_path = os.path.join(self.base_path, bucket_name, key)
        os.makedirs(object_path, exist_ok=True)

        version = self.getNextVersion(object_path)

        version_path = os.path.join(object_path, version)
        os.makedirs(version_path, exist_ok=True)

        for i, chunk in enumerate(chunks):
            with open(os.path.join(version_path, f"chunk{i+1}"), "wb") as f:
                f.write(chunk)

        print(f"Stored in {bucket_name}/{version}")

    def getNextVersion(self, object_path):
        if not os.path.exists(object_path):
            return "v1"

        versions = [d for d in os.listdir(object_path) if d.startswith('v')]
        if not versions:
            return "v1"

        latest_version = max(int(v[1:]) for v in versions)
        return f"v{latest_version + 1}"


    def getObject(self, bucket_name, key, version=None):

        object_path = os.path.join(self.base_path, bucket_name, key)

        if version is None:
            version = self.getLatestVersion(object_path)

        version_path = os.path.join(object_path, version)

        with open("output.jpg", "wb") as out:
            i = 1
            while True:
                chunk_path = os.path.join(version_path, f"chunk{i}")

                if not os.path.exists(chunk_path):
                    break

                with open(chunk_path, "rb") as f:
                    out.write(f.read())

                i += 1

        print("Retrieved as output.jpg")
        
    def getLatestVersion(self, object_path):

        versions = [d for d in os.listdir(object_path) if d.startswith("v")]

        if not versions:
            return None

        latest = max(int(v[1:]) for v in versions)
        return f"v{latest}"

    def listObjects(self, bucket_name, prefix):

        bucket_path = os.path.join(self.base_path, bucket_name)

        if not os.path.exists(bucket_path):
            print("Bucket does not exist.")
            return


        object_keys = []

        for root, dirs, files in os.walk(bucket_path):


            if any(d.startswith("v") for d in root.split(os.sep)):
                continue


            if not dirs:
                key = os.path.relpath(root, bucket_path)
                object_keys.append(key)


        if prefix:
            object_keys = [k for k in object_keys if k.startswith(prefix)]

        print("Objects:")
        for key in object_keys:
            print(key)