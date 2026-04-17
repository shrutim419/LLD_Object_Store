from src.object_store import ObjectStore


def main():
    store = ObjectStore(chunk_size=1000000)

    # Create buckets
    store.createBucket("photos")
    store.createBucket("documents")

    # Store objects with prefixes
    store.putObject("photos", "trip/cat.mp4", "test_files/cat.mp4")
    store.putObject("photos", "trip/Taj Mahal.jpg", "test_files/Taj Mahal.jpg")

    store.putObject("photos", "gifs/giphy.gif", "test_files/giphy.gif")
    store.putObject("photos", "gifs/pedro.gif", "test_files/pedro.gif")

    store.putObject("documents", "sample.txt", "test_files/sample.txt")

    # Create a new version
    store.putObject("photos", "gifs/pedro.gif", "test_files/pedro.gif")

    # Demonstrate getLatestVersion() utility
    print("\n--- Demonstrating getLatestVersion() ---")
    object_path = "object-store/photos/gifs/pedro.gif"
    latest = store.getLatestVersion(object_path)
    print(f"Latest version available: {latest}")

    # Retrieve
    store.getObject("photos", "gifs/pedro.gif")
    store.getObject("photos", "gifs/pedro.gif", "v1")

    # List
    store.listObjects("photos")
    store.listObjects("photos", prefix="trip/")
    store.listObjects("photos", prefix="gifs/")


if __name__ == "__main__":
    main()