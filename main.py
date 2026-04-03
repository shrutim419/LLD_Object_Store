from src.object_store import ObjectStore
import os


def create_sample_file():

    with open("sample.txt", "w") as f:
        f.write("This is a test file for object store.\n" * 50)


def main():

    store = ObjectStore()

    store.createBucket("photos")

    chunks = [b"hello ", b"world ", b"test"]

    store.putObject("photos", "2025/img1.jpg", chunks)
    store.putObject("photos", "2025/img2.jpg", chunks)

    store.getObject("photos", "2025/img1.jpg")
    store.listObjects("photos")

if __name__ == "__main__":
    main()