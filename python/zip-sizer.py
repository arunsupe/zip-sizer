#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This script, zip-sizer.py, is a python script that estimates the
# compressed size of large directories. It works by sampling a fraction of
# the data to efficiently calculate the compression ratio.
# The sampling fraction can be specified using the --sampling-ratio command line flag.
# Only uses stdlib modules and does not require any third-party libraries.
# This is the python (much slower) version of the original go script, zip-sizer.go

import os
import gzip
import bz2
import sys
import argparse
import logging

CHUNKSIZE = 10 * 1024 * 1024


def list_files_with_sizes(directory):
    """
    List all files in a directory and its subdirectories along with their sizes.
    """
    files_with_sizes = []
    for root, _, files in os.walk(directory):
        for file in files:
            filepath = os.path.join(root, file)

            # only count files that are not directories or symbolic links
            if (
                not os.path.isfile(filepath)
                or os.path.isdir(filepath)
                or os.path.islink(filepath)
            ):
                continue
            try:
                filesize = os.path.getsize(filepath)
                files_with_sizes.append((filepath, filesize))
            except OSError as e:
                logging.warning(
                    f"Error getting size of file {filepath}: {e}", file=sys.stderr
                )
                continue
            except FileNotFoundError as e:
                logging.warning(f"File not found: {filepath}: {e}", file=sys.stderr)
                continue
    return files_with_sizes


def sample_data(files_with_sizes):
    """
    Sample the last `sampling_ratio` fraction of bytes for every `CHUNKSIZE` bytes from the collated data.
    """

    total_size = sum(size for _, size in files_with_sizes)
    if args.verbose:
        print(f"Total number of files: {len(files_with_sizes)}")
        print(
            f"Total size of files: {total_size} bytes ({human_readable_size(total_size)})"
        )

    sample_size = int(args.sampling_ratio * CHUNKSIZE)
    if args.verbose:
        print(f"Sampling {sample_size} bytes from every {CHUNKSIZE} bytes of data.")

    sampled_data = b""

    # Determine sampling points
    sampling_points = list(range(CHUNKSIZE - sample_size, total_size, CHUNKSIZE))

    # Iterate through files and extract data
    current_offset = 0
    for filepath, filesize in files_with_sizes:
        # Skip files that don't contain any sampling points
        if not sampling_points or sampling_points[0] >= current_offset + filesize:
            current_offset += filesize
            continue

        # Open the file only if it contains relevant sampling points
        try:
            with open(filepath, "rb") as f:
                while (
                    sampling_points and current_offset + filesize > sampling_points[0]
                ):
                    # Calculate relative offset within the current file
                    relative_offset = sampling_points[0] - current_offset
                    f.seek(relative_offset)
                    sampled_data += f.read(sample_size)
                    sampling_points.pop(0)  # Move to the next sampling point

            current_offset += filesize
        except FileNotFoundError:
            logging.warning(f"Error: The file '{filepath}' does not exist.")
            continue
        except IOError:
            logging.warning(f"Error: Unable to read the file '{filepath}'.")
            continue
        except PermissionError:
            logging.warning(
                f"Error: Insufficient permissions to read the file '{filepath}'."
            )
            continue
        except IsADirectoryError:
            logging.warning(f"Error: '{filepath}' is a directory, not a file.")
            continue
        except TypeError:
            logging.warning(
                "Error: Invalid file path type. Expected a string, bytes, or os.PathLike object."
            )
            continue
        except OSError as e:
            logging.warning(f"OS error occurred: {e}")
            continue
    return sampled_data


def compress_sampled_data(sampled_data):
    """
    Compress the sampled data using gzip.
    """
    if args.algorithm == "bzip2":
        compressed_data = bz2.compress(sampled_data, compresslevel=args.level)
    elif args.algorithm == "gzip":
        compressed_data = gzip.compress(sampled_data, compresslevel=args.level)

    if args.verbose:
        print(
            f"Compressed sample size: {len(compressed_data)} bytes ({human_readable_size(len(compressed_data))})"
        )
        print(
            f"Original sample size: {len(sampled_data)} bytes ({human_readable_size(len(sampled_data))})"
        )
    return len(compressed_data)


def main(directory):
    # Step 1: List all files and their sizes
    files_with_sizes = list_files_with_sizes(directory)

    # Step 2: Sample data
    sampled_data = sample_data(files_with_sizes)

    # Step 3: Compress sampled data
    compressed_size = compress_sampled_data(sampled_data)

    # Step 4: Calculate compression ratio
    original_size = sum(size for _, size in files_with_sizes)
    compressed_size = (
        (float(compressed_size) / len(sampled_data)) * original_size
        if original_size > 0
        else 0
    )

    # Step 5: Print original and estimated compressed size
    print(
        f"---------------------------------------------------------------------------------"
    )
    print(
        f"Original size: {original_size} bytes ({human_readable_size(original_size)})"
    )
    print(
        f"Estimated compressed size: {compressed_size:.2f} ({human_readable_size(compressed_size)}) "
    )
    print(f"Compression ratio: {(original_size / compressed_size):.2f}")
    print(
        f"---------------------------------------------------------------------------------"
    )


def human_readable_size(size):
    """
    Convert a size in bytes to a human-readable format.
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Estimate compression size of sampled data."
    )
    parser.add_argument("directory", nargs="?", help="Directory to process.")
    parser.add_argument(
        "--algorithm",
        choices=["gzip", "bzip2"],
        default="gzip",
        help="Compression algorithm to use (gzip or bzip2).",
    )
    parser.add_argument(
        "--level",
        type=int,
        choices=range(1, 10),
        default=9,
        help="Compression level (1-9).",
    )
    parser.add_argument(
        "--sampling-ratio",
        type=float,
        default=0.1,
        help="Sampling ratio (fraction of data to sample, e.g., 0.1 for 10%%).",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    args = parser.parse_args()

    # if no directory is provided,  print the help message and exit
    if args.directory is None:
        parser.print_help()
        sys.exit(1)

    if args.sampling_ratio <= 0 or args.sampling_ratio > 1:
        logging.critical("Sampling ratio must be between 0 and 1.")
        sys.exit(1)

    if args.level < 1 or args.level > 9:
        logging.critical("Compression level must be between 1 and 9.")
        sys.exit(1)

    if args.algorithm not in ["gzip", "bzip2"]:
        logging.critical("Invalid compression algorithm. Choose 'gzip' or 'bzip2'.")
        sys.exit(1)

    if not os.path.isdir(args.directory):
        logging.critical(f"Provided path '{args.directory}' is not a directory.")
        sys.exit(1)

    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    main(directory=args.directory)
