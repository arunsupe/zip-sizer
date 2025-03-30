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

// FileInfo struct to hold file path and size
type FileInfo struct {
	Path string
	Size int64
}

// Args struct to hold command line arguments
type Args struct {
	Directory            string  `arg:"positional,required" help:"Directory to scan for files"`
	CompressionLevel     int     `arg:"-l,--compression-level" help:"Compression level (1-9)"`
	CompressionAlgorithm string  `arg:"-a,--compression-algorithm" help:"Compression algorithm (gzip or bzip2)"`
	SampleRatio          float64 `arg:"-r,--sample-ratio" help:"Sample ratio for compression estimation"`
	HumanReadable        bool    `arg:"-u,--human-readable" help:"Display sizes in human-readable format"`
}

var totalSize int64

// List all files in a directory and send their sizes
// Send it down a channel as it arrives
// This is done to avoid loading all file sizes into memory at once
func listFilesWithSizes(directory string, fileInfoChan chan<- FileInfo) {
	defer close(fileInfoChan)

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error accessing path %s: %v\n", path, err)
			return nil // Log the error and continue
		}
		if !info.IsDir() {
			fileInfoChan <- FileInfo{Path: path, Size: info.Size()}
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

// Sample sampleSize bytes from every chunkSize from the concatenated file stream
// The basic idea is to pretend the files are a single large file and sample data from it
// at regular intervals. This is done by calculating the offsets of the sampled data in the
// concatenated file and then reading the data from the original files at those offsets.
// Extract sampled data from the original files and write it to a pipe
// This allows us to stream the sampled data without loading all files into memory at once
func streamSampledData(fileInfoChan <-chan FileInfo, chunkSize, sampleSize int64) (io.Reader, error) {
	sampledDataPipe, sampledDataWriter := io.Pipe()

	go func() {
		defer sampledDataWriter.Close()

		totalSize = 0
		currentOffset := int64(0)
		nextSamplePoint := chunkSize - sampleSize // Initialize the first sample point

		for file := range fileInfoChan {
			totalSize += file.Size

			if nextSamplePoint >= currentOffset+file.Size {
				currentOffset += file.Size
				continue
			}

			f, err := os.Open(file.Path)
			if err != nil {
				sampledDataWriter.CloseWithError(err)
				return
			}
			defer f.Close()

			for nextSamplePoint < currentOffset+file.Size {
				relativeOffset := nextSamplePoint - currentOffset
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

				nextSamplePoint += chunkSize
			}

			currentOffset += file.Size
		}
	}()

	return sampledDataPipe, nil
}

// Compress data using a specified compression writer (supports gzip and bzip2)
// compress the data from the sampled data stream, not saving the compressed data; just the compressed size
// The compression ratio is calculated as the size of the compressed data divided by the size of the uncompressed data
// The function returns the compression ratio as a float64
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

// Validate the command line arguments
func validateArgs(args Args) error {
	if stat, err := os.Stat(args.Directory); err != nil || !stat.IsDir() {
		fmt.Printf("Provided path '%s' is not a directory.\n", args.Directory)
		os.Exit(1)
	}

	// Check if the sample ratio is valid
	if args.SampleRatio <= 0 || args.SampleRatio > 1 {
		fmt.Printf("Sample ratio must be between 0 and 1.\n")
		os.Exit(1)
	}
	// Check if the compression level is valid
	if args.CompressionLevel < 1 || args.CompressionLevel > 9 {
		fmt.Printf("Compression level must be between 1 and 9.\n")
		os.Exit(1)
	}
	// Check if the compression algorithm is valid
	if args.CompressionAlgorithm != "gzip" && args.CompressionAlgorithm != "bzip2" {
		fmt.Printf("Compression algorithm must be 'gzip' or 'bzip2'.\n")
		os.Exit(1)
	}

	return nil
}

// Convert bytes to human-readable format
func convertToHumanReadable(size int64) string {

	sizeFloat := float64(size)

	units := []string{"B", "KB", "MB", "GB", "TB"}
	index := 0
	for sizeFloat >= 1024 && index < len(units)-1 {
		sizeFloat /= 1024
		index++
	}
	return fmt.Sprintf("%.2f %s", float64(sizeFloat), units[index])
}

func main() {
	var args Args
	args.CompressionLevel = COMPRESSION_LEVEL
	args.CompressionAlgorithm = "gzip"
	args.SampleRatio = 0.1
	arg.MustParse(&args)

	// Validate the arguments
	if err := validateArgs(args); err != nil {
		fmt.Printf("Error validating arguments: %v\n", err)
		os.Exit(1)
	}

	// Calculate the sample size based on the sample ratio
	sampleSize := int64(float64(CHUNKSIZE) * args.SampleRatio)

	// Create a channel to receive file sizes
	fileInfoChan := make(chan FileInfo)

	// Start a goroutine to list files and send their sizes to the channel
	go listFilesWithSizes(args.Directory, fileInfoChan)

	// Stream the sampled data from the files
	sampledData, err := streamSampledData(fileInfoChan, CHUNKSIZE, sampleSize)
	if err != nil {
		fmt.Printf("Error streaming sampled data: %v\n", err)
		os.Exit(1)
	}

	// Compress the sampled data and calculate the compression ratio
	compressedRatio, err := compressData(
		sampledData,
		args.CompressionLevel,
		args.CompressionAlgorithm,
	)
	if err != nil {
		fmt.Printf("Error during compression: %v\n", err)
		os.Exit(1)
	}

	// Calculate the estimated compressed size based on the total size and compression ratio
	estimatedCompressedSize := int64(float64(totalSize) * compressedRatio)
	if args.HumanReadable {
		fmt.Printf("Total original size: %s\n", convertToHumanReadable(totalSize))
		fmt.Printf("Estimated compressed size: %s\n", convertToHumanReadable(estimatedCompressedSize))
	} else {
		fmt.Printf("Total original size: %d bytes\n", totalSize)
		fmt.Printf("Estimated compressed size: %d bytes\n", estimatedCompressedSize)
	}
}
