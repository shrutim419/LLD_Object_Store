from src.object_store import ObjectStore


def main():
    store = ObjectStore()

    # Create buckets
    store.createBucket("photos")
    store.createBucket("documents")

    # Store objects with prefixes
    store.putObject("photos", "trip/cat.mp4", "cat.mp4")
    store.putObject("photos", "trip/Taj Mahal.jpg", "Taj Mahal.jpg")

    store.putObject("photos", "gifs/giphy.gif", "giphy.gif")
    store.putObject("photos", "gifs/pedro.gif", "pedro.gif")

    store.putObject("documents", "sample.txt", "sample.txt")

    # Create a new version
    store.putObject("photos", "gifs/pedro.gif", "pedro.gif")

    # Retrieve
    store.getObject("photos", "gifs/pedro.gif")
    store.getObject("photos", "gifs/pedro.gif", "v1")

    # List
    store.listObjects("photos")
    store.listObjects("photos", prefix="trip/")
    store.listObjects("photos", prefix="gifs/")


if __name__ == "__main__":
    main()