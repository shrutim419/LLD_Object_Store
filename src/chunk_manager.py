import os
import hashlib
# import meta_data_manager

class ChunkManager:
    """
    A utility class for managing data chunking,chunk storage, and integrity check.

    Attributes:
        chunk_size (int): The maximum size of each data chunk in bytes. (1MB = 10000000)
    """
    def __init__(self, chunk_size: int):
        self.chunk_size = chunk_size

    def split_into_chunks(self, data: bytes):
        chunks = []
        print("Splitting data into chunks")
        for i in range(0, len(data), self.chunk_size):
                chunks.append(data[i:i+self.chunk_size])
        print(f'Data split into {len(chunks)} chunks')
        return chunks
    
    def write_chunks(self, version_path: str, chunks: list[bytes]):
        for i, chunk_data in enumerate(chunks):
             filename = f'chunk{i+1}'
             filepath = os.path.join(version_path, filename)
             with open(filepath, 'wb') as f:
                  f.write(chunk_data)

    def read_chunks(self, version_path: str, chunk_count: int):
        merged_data = bytearray()
        print(f'Reading {chunk_count} chunks from {version_path}')
        for i in range(1, chunk_count + 1):
            filename = f'chunk{i}'
            filepath = os.path.join(version_path, filename)

            if not os.path.exists(filepath):
                 raise FileNotFoundError(f'Missing Chunk: {filepath}')
            
            with open(filepath, 'rb') as f:
                 merged_data.extend(f.read())
        return bytes(merged_data)

    def compute_hash(self, data: bytes):
        filehash = hashlib.sha256(data).hexdigest()
        return filehash
    
if __name__ == "__main__":
    # Example use
    chunk_size = 10000000 # 1 MB
    chunk_mgr = ChunkManager(chunk_size)
    data = "README.txt"
    with open(data, 'rb') as f:
        read_data = f.read()
    chunk_mgr.compute_hash(read_data)
    chunks = chunk_mgr.split_into_chunks(read_data)
    chunk_count = len(chunks)
    chunk_mgr.write_chunks("V1", chunks)
    reconstructed_data = chunk_mgr.read_chunks("V1", chunk_count)
    chunk_mgr.compute_hash(reconstructed_data)