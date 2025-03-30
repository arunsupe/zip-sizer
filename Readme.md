# zip-sizer

`zip-sizer` is a command-line tool that estimates the compressed size of large directories. It works by sampling a fraction of the data to efficiently calculate the compression ratio. 

## Features
- very __`memory efficient`__ and __`fast`__
- supports estimates for the `gzip` and `bzip2` algorithms
- estimate for different compression levels (1-9)
- Accuracy is about +/- 2.5% in my testing, but will obviously depend on type of files, size of the archive and sampling fraction.

## Example Usage

To estimate the compressed size of the ~/Downloads directory using gzip with a compression level set to 5 and sampling 10% of the data, with human-readable size reporting:
```bash
> bin/zip-sizer -l 5 -a gzip -r 0.1 --human-readable ~/Downloads 

# Output
Total original size: 3.73 GB
Estimated compressed size: 3.46 GB

# It is fast enough to be useful
> time bin/zip-sizer -l 5 -a gzip -r 0.1 ~/Downloads

# Output
Total original size: 4003741457 bytes
Estimated compressed size: 3711794335 bytes
bin/zip-sizer -l 5 -a gzip -r 0.1 ~/Downloads  10.30s user 0.34s system 106% cpu 9.952 total

```

## Installation

### Prerequisites

-   Go (version 1.16 or later) must be installed on your system.

### Clone and build

```bash
git clone https://github.com/arunsupe/zip-sizer.git
cd zip-sizer
go build -o bin/zip-sizer zip-sizer.go
```

## Usage

Run the program with the following command-line options:

```bash
./bin/zip-sizer [options] <directory>
```

## Positional Arguments

    <directory>: The directory to estimate the compressed size of.

## Options

    -l, --compression-level: Compression level (1-9). Default: 9.
    -a, --compression-algorithm: Compression algorithm (gzip or bzip2). Default: gzip.
    -r, --sample-ratio: Sample ratio for compression estimation (e.g., 0.1 for 10%). Default: 0.1.
    -u, --human-readable: Display sizes in human-readable format.

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