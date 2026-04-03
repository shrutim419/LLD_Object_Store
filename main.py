from src.object_store import ObjectStore
import os


def create_sample_file():

    with open("sample.txt", "w") as f:
        f.write("This is a test file for object store.\n" * 50)


def main():

    store = ObjectStore()

    store.createBucket("photos")
    store.createBucket("Documents")

    store.putObject("photos", "2025/cat.gif", 'giphy.gif')
    store.putObject("photos", "2025/pedro.gif", 'pedro.gif')
    store.putObject("photos", "2025/cat.mp4", 'cat.mp4')
    store.putObject("Documents", "sample.txt", "sample.txt")

    store.getObject("photoss", "2025/pedro.gif")
    store.getObject("photos", "2025/pedro.gif")
    store.getObject("photos", "2025/cat.mp4")
    store.getObject("Documents", "sample.txt")
    store.listObjects("photos")

if __name__ == "__main__":
    main()