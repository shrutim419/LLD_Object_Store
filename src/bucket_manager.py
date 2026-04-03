import os

class BucketManager:
    def __init__(self, base_path):
        self.base_path = base_path

    def create_bucket(self, bucket_name):
        bucket_path = os.path.join(self.base_path, bucket_name)
        
        if os.path.exists(bucket_path):
            print("Bucket already exists.")
            return False

        os.makedirs(bucket_path)
        print(f"Bucket {bucket_name} created successfully.")
        return True

    def bucket_exists(self, bucket_name):
        bucket_path = os.path.join(self.base_path, bucket_name)
        return os.path.exists(bucket_path)