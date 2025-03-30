# Estimate Compression

`estimate-compression` is a command-line tool that estimates the compressed size of large directories. It works by intelligently sampling a fraction of the data to efficiently calculate the compression ratio. It supports gzip and bzip2. It is very __`memory efficient`__, and __`fast`__. Accuracy is about +/- 2.5% in my testing.

## Example Usage

Estimate the compressed size of files in the data directory using gzip with a compression level of 5 and a sample ratio of 20%:
```bash
> bin/estimate-compression -l 5 -a gzip -r 0.1 ~/Downloads 

# Output
Total original size: 4003741457 bytes
Estimated compressed size: 3711794335 bytes


# It is fast enough to be useful
> time bin/estimate-compression -l 5 -a gzip -r 0.1 ~/Downloads

# Output
Total original size: 4003741457 bytes
Estimated compressed size: 3711794335 bytes
bin/estimate-compression -l 5 -a gzip -r 0.1 ~/Downloads  10.30s user 0.34s system 106% cpu 9.952 total

```

## Installation

### Prerequisites

-   Go (version 1.16 or later) must be installed on your system.

### Steps

1.  **Clone the Repository:**

```bash
git clone [https://github.com/arunsupe/estimate-compression.git](https://github.com/arunsupe/estimate-compression.git)
cd estimate-compression
```

2.  **Build the Binary:**

```bash
go build -o bin/estimate-compression estimate-compression.go
```

## Usage

Run the program with the following command-line options:

```bash
./bin/estimate-compression <directory> [options]
```

## Positional Arguments

    <directory>: The directory to scan for files.

## Options

    -l, --compression-level: Compression level (1-9). Default: 9.
    -a, --compression-algorithm: Compression algorithm (gzip or bzip2). Default: gzip.
    -r, --sample-ratio: Sample ratio for compression estimation (e.g., 0.1 for 10%). Default: 0.1.


## Output

The program provides the following output:

    Total original size of the files in bytes.
    Estimated compressed size in bytes.

## Example Output
```bash
Total original size: 104857600 bytes
Estimated compressed size: 52428800 bytes
```

## Dependencies

This project uses the following Go libraries:

    github.com/alexflint/go-arg for argument parsing.
    github.com/dsnet/compress for bzip2 compression.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Author

Created by arunsupe.