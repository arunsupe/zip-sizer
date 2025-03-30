package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dsnet/compress/bzip2"

	"github.com/alexflint/go-arg"
)

const (
	CHUNKSIZE         = 10 * 1024 * 1024 // 10 MB
	COMPRESSION_LEVEL = int(9)
)

type FileInfo struct {
	Path string
	Size int64
}

// List all files in a directory and their sizes
func listFilesWithSizes(directory string) ([]FileInfo, error) {
	var filesWithSizes []FileInfo

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error accessing path %s: %v\n", path, err)
			return nil // Log the error and continue
		}
		if !info.IsDir() {
			filesWithSizes = append(filesWithSizes, FileInfo{Path: path, Size: info.Size()})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return filesWithSizes, nil
}

// Stream sampled data
// The basic idea is to pretend the files are a single large file and sample data from it
// at regular intervals. This is done by calculating the offsets of the sampled data in the
// concatenated file and then reading the data from the original files at those offsets.
func streamSampledData(filesWithSizes []FileInfo, chunkSize, sampleSize int64) (io.Reader, error) {
	totalSize := int64(0)
	for _, file := range filesWithSizes {
		totalSize += file.Size
	}

	samplingPoints := make([]int64, 0)
	for i := chunkSize - sampleSize; i < totalSize; i += chunkSize {
		samplingPoints = append(samplingPoints, i)
	}

	currentOffset := int64(0)
	sampledDataPipe, sampledDataWriter := io.Pipe()

	go func() {
		defer sampledDataWriter.Close()

		for _, file := range filesWithSizes {
			if len(samplingPoints) == 0 || samplingPoints[0] >= currentOffset+file.Size {
				currentOffset += file.Size
				continue
			}

			f, err := os.Open(file.Path)
			if err != nil {
				sampledDataWriter.CloseWithError(err)
				return
			}
			defer f.Close()

			for len(samplingPoints) > 0 && currentOffset+file.Size > samplingPoints[0] {
				relativeOffset := samplingPoints[0] - currentOffset
				if _, err := f.Seek(relativeOffset, io.SeekStart); err != nil {
					sampledDataWriter.CloseWithError(err)
					return
				}

				buf := make([]byte, sampleSize)
				n, err := f.Read(buf)
				if err != nil && err != io.EOF {
					sampledDataWriter.CloseWithError(err)
					return
				}

				if n > 0 {
					if _, err := sampledDataWriter.Write(buf[:n]); err != nil {
						sampledDataWriter.CloseWithError(err)
						return
					}
				}

				samplingPoints = samplingPoints[1:]
			}

			currentOffset += file.Size
		}
	}()

	return sampledDataPipe, nil
}

// Compress data using a specified compression writer (supports gzip and bzip2)
func compressData(uncompressedInput io.Reader, compressionLevel int, compressionAlgorithm string) (float64, error) {
	compressedSize := float64(0)
	uncompressedSize := float64(0)

	// Create a pipe to stream the compressed data
	// Write compressed data directly into the pipe
	// Read the compressed data size from the other end
	compressedDataPipe, compressedDataWriter := io.Pipe()

	go func() {
		var writer io.WriteCloser
		var err error

		// Set the compression algorithm and level
		switch compressionAlgorithm {
		case "bzip2":
			writer, err = bzip2.NewWriter(compressedDataWriter, &bzip2.WriterConfig{Level: compressionLevel}) // Requires "github.com/dsnet/compress/bzip2"
			if err != nil {
				compressedDataWriter.CloseWithError(err)
				return
			}
		default: // Default to gzip
			writer, err = gzip.NewWriterLevel(compressedDataWriter, compressionLevel)
			if err != nil {
				compressedDataWriter.CloseWithError(err)
				return
			}
		}

		defer writer.Close()
		defer compressedDataWriter.Close()

		buf := make([]byte, 4096)
		for {
			// Read from the uncompressed input stream into the buffer
			n, err := uncompressedInput.Read(buf)
			if n > 0 {
				// keep track of the uncompressed size (to calculate the compression ratio)
				uncompressedSize += float64(n)
				if _, err := writer.Write(buf[:n]); err != nil {
					compressedDataWriter.CloseWithError(err)
					return
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				compressedDataWriter.CloseWithError(err)
				return
			}
		}
	}()

	buf := make([]byte, 4096)
	for {
		n, err := compressedDataPipe.Read(buf)
		compressedSize += float64(n)

		if err == io.EOF {
			break
		}
		if err != nil {
			return compressedSize, err
		}
	}

	return compressedSize / uncompressedSize, nil
}

type Args struct {
	Directory            string  `arg:"positional,required" help:"Directory to scan for files"`
	CompressionLevel     int     `arg:"-l,--compression-level" help:"Compression level (1-9)"`
	CompressionAlgorithm string  `arg:"-a,--compression-algorithm" help:"Compression algorithm (gzip or bzip2)"`
	SampleRatio          float64 `arg:"-r,--sample-ratio" help:"Sample ratio for compression estimation"`
}

func main() {
	var args Args
	args.CompressionLevel = COMPRESSION_LEVEL
	args.CompressionAlgorithm = "gzip"
	args.SampleRatio = 0.1
	arg.MustParse(&args)

	if stat, err := os.Stat(args.Directory); err != nil || !stat.IsDir() {
		fmt.Printf("Provided path '%s' is not a directory.\n", args.Directory)
		os.Exit(1)
	}

	filesWithSizes, err := listFilesWithSizes(args.Directory)
	if err != nil {
		fmt.Printf("Error listing files: %v\n", err)
		os.Exit(1)
	}

	totalOriginalSize := int64(0)
	for _, file := range filesWithSizes {
		totalOriginalSize += file.Size
	}

	sampleSize := int64(float64(CHUNKSIZE) * args.SampleRatio)
	sampledData, err := streamSampledData(filesWithSizes, CHUNKSIZE, sampleSize)
	if err != nil {
		fmt.Printf("Error streaming sampled data: %v\n", err)
		os.Exit(1)
	}
	compressedRatio, err := compressData(
		sampledData,
		args.CompressionLevel,
		args.CompressionAlgorithm,
	)
	if err != nil {
		fmt.Printf("Error during compression: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Total original size: %d bytes\n", totalOriginalSize)
	fmt.Printf("Estimated compressed size: %d bytes\n", int64(float64(totalOriginalSize)*compressedRatio))
}
