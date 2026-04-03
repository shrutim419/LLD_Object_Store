from src.object_store import ObjectStore
from src.bucket_manager import BucketManager
from src.object_manager import ObjectManager

store = ObjectStore()
bucket_mgr = BucketManager(store.storage_path)
object_mgr = ObjectManager(store.)

bucket_mgr.create_bucket("photos")

chunks = [b"chunk1", b"chunk2", b"chunk3"]

object_mgr.put_object("photos", "2025/kokan/img1.jpg", chunks)
object_mgr.put_object("photos", "2025/kokan/img2.jpg", chunks)

