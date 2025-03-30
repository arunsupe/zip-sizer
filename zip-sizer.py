import os
import gzip
import bz2
import sys

def list_files_with_sizes(directory):
    """
    List all files in a directory and its subdirectories along with their sizes.
    """
    files_with_sizes = []
    for root, _, files in os.walk(directory):
        for file in files:
            filepath = os.path.join(root, file)
            filesize = os.path.getsize(filepath)
            files_with_sizes.append((filepath, filesize))
    return files_with_sizes

def sample_data(files_with_sizes, chunk_size=10 * 1024 * 1024, sample_size=1 * 1024 * 1024):
    """
    Sample the last `sample_size` bytes for every `chunk_size` bytes from the collated data.
    """
    sampled_data = b""
    total_size = sum(size for _, size in files_with_sizes)
    
    # Determine sampling points
    sampling_points = list(range(chunk_size - sample_size, total_size, chunk_size))
    
    # Iterate through files and extract data
    current_offset = 0
    for filepath, filesize in files_with_sizes:
        # Skip files that don't contain any sampling points
        if not sampling_points or sampling_points[0] >= current_offset + filesize:
            current_offset += filesize
            continue

        # Open the file only if it contains relevant sampling points
        with open(filepath, 'rb') as f:
            while sampling_points and current_offset + filesize > sampling_points[0]:
                # Calculate relative offset within the current file
                relative_offset = sampling_points[0] - current_offset
                f.seek(relative_offset)
                sampled_data += f.read(sample_size)
                sampling_points.pop(0)  # Move to the next sampling point

        current_offset += filesize

    return sampled_data

def compress_sampled_data(sampled_data):
    """
    Compress the sampled data using gzip.
    """
    compressed_data = gzip.compress(sampled_data, compresslevel=9)
    # Optionally, you can use bz2 for better compression
    # compressed_data = bz2.compress(sampled_data, compresslevel=9)
    return len(compressed_data)

def main(directory):
    # Step 1: List all files and their sizes
    files_with_sizes = list_files_with_sizes(directory)
    
    # Step 2: Sample data
    sampled_data = sample_data(files_with_sizes)
    
    print(f"Sampled {len(sampled_data)} bytes of data.")

    # Step 3: Compress sampled data
    compressed_size = compress_sampled_data(sampled_data)

    # Step 4: Calculate compression ratio
    original_size = sum(size for _, size in files_with_sizes)
    compressed_size = (compressed_size / len(sampled_data)) * original_size if original_size > 0 else 0
    print(f"Compressed size: {compressed_size:.2f}")

if __name__ == "__main__":
    directory = sys.argv[1] if len(sys.argv) > 1 else '.'
    if not os.path.isdir(directory):
        print(f"Provided path '{directory}' is not a directory.")
        sys.exit(1)
    main(directory=directory)
