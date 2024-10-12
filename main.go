package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
)

const (
	IndexInterval = 1000			// Store offset every 1000 lines
	MaxLineSize   = 1000 + 1  // Including newline character assume it's \n
)

type IndexEntry struct {
	LineNumber int64
	Offset     int64
}

type ChunkInfo struct {
	StartOffset int64
	EndOffset   int64
	ChunkSize   int64
	PrefixSum   int64 // Cumulative size of all preceding chunks
}

func main() {
	// Argument validation
	if len(os.Args) != 3 {
		fmt.Println("Help: main.go ./<input_file> <line_number>")
		os.Exit(1)
	}

	// Extract arguments
	inputFilePath := os.Args[1]
	lineNumberStr := os.Args[2]

	// Parse the line arg into an int64
	lineNumber, err := strconv.ParseInt(lineNumberStr, 10, 64)
	if err != nil || lineNumber < 0 {
		fmt.Println("Line number must be a positive integer.")
		os.Exit(1)
	}

	indexFilePath := inputFilePath + ".idx"

	// Check if we already have an index file, if not create a one
	if _, err := os.Stat(indexFilePath); os.IsNotExist(err) {
		fmt.Printf("Writing index to %s... \n", indexFilePath)
		
		err = buildIndex(inputFilePath, indexFilePath)
		if err != nil {
			fmt.Printf("Error building index: %v\n", err)
			os.Exit(1)
		}
	}

	err = findAndPrintLine(inputFilePath, indexFilePath, lineNumber)
	if err != nil {
		fmt.Printf("Error printing line: %v\n", err)
		os.Exit(1)
	}
}

func buildIndex(inputFilePath, indexFilePath string) error {
	// Num of parallel workers based on the number of logical CPUs
	numWorkers := runtime.NumCPU()

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	fileInfo, err := inputFile.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// Get chunk info
	chunks, err := getChunks(inputFile, fileSize, numWorkers)
	if err != nil {
		return err
	}

	// Calc prefix sums for chunk offsets
	var prefixSum int64 = 0
	for i := range chunks {
		chunks[i].PrefixSum = prefixSum
		prefixSum += chunks[i].ChunkSize
	}

	// Crate chan to collect index entries from workers
	indexChan := make(chan []IndexEntry, numWorkers)
	var wg sync.WaitGroup

	// Start workers
	for _, chunk := range chunks {
		wg.Add(1)
		
		go func(chunk ChunkInfo) {
			defer wg.Done()
			
			entries, err := processChunk(inputFilePath, chunk)
			if err != nil {
				fmt.Printf("Error processing chunk: %v\n", err)
				os.Exit(1)
			}
			
			indexChan <- entries
		}(chunk)
	}

	// Close channel when all workers are done
	go func() {
		wg.Wait()
		close(indexChan)
	}()

	// Collect index entries
	var allEntries []IndexEntry
	for entries := range indexChan {
		allEntries = append(allEntries, entries...)
	}

	// Sort the index entries by line number
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].LineNumber < allEntries[j].LineNumber
	})

	// Write the index entries to the index file
	indexFile, err := os.Create(indexFilePath)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	for _, entry := range allEntries {
		err = binary.Write(indexFile, binary.LittleEndian, entry.LineNumber)
		if err != nil {
			return err
		}
		
		err = binary.Write(indexFile, binary.LittleEndian, entry.Offset)
		if err != nil {
			return err
		}
	}

	return nil
}

func getChunks(inputFile *os.File, fileSize int64, numChunks int) ([]ChunkInfo, error) {
	var chunks []ChunkInfo
	chunkSize := fileSize / int64(numChunks)

	var offset int64 = 0
	for i := 0; i < numChunks; i++ {
		var startOffset int64 = offset
		var endOffset int64

		if i == numChunks-1 {
			// Last chunk goes to the end of the file
			endOffset = fileSize
		} else {
			endOffset = startOffset + chunkSize
		}

		// Adjust endOffset to the end of the current line
		endOffset, err := adjustOffset(inputFile, endOffset, fileSize)
		if err != nil {
			return nil, err
		}

		// Create chunk
		chunk := ChunkInfo{
			StartOffset: startOffset,
			EndOffset:   endOffset,
			ChunkSize:   endOffset - startOffset,
		}

		chunks = append(chunks, chunk)
		offset = endOffset
	}

	return chunks, nil
}

func adjustOffset(file *os.File, offset int64, fileSize int64) (int64, error) {
	if offset >= fileSize {
		return fileSize, nil
	}

	// Seek to the offset
	_, err := file.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

	reader := bufio.NewReader(file)
	// Read until the end of the line
	_, err = reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return 0, err
	}

	newOffset, err := file.Seek(0, 1) // Get current offset
	if err != nil {
		return 0, err
	}

	return newOffset, nil
}

func processChunk(filePath string, chunk ChunkInfo) ([]IndexEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Seek to the start of the chunk
	_, err = file.Seek(chunk.StartOffset, 0)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(io.LimitReader(file, chunk.EndOffset-chunk.StartOffset))

	var entries []IndexEntry
	var offset int64 = chunk.StartOffset
	var lineNumber int64 = 0

	// If this is not the first chunk, adjust the line number
	if chunk.PrefixSum > 0 {
		// Read until the first newline
		_, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		
		offset, _ = file.Seek(0, 1)
	}

	// Start line numbers from the appropriate value
	lineNumber = (offset - chunk.PrefixSum) / MaxLineSize

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					// Handle last line without newline
					if lineNumber%IndexInterval == 0 {
						entries = append(entries, IndexEntry{
							LineNumber: lineNumber,
							Offset:     offset,
						})
					}
					
					offset += int64(len(line))
				}
				break
			} else {
				return nil, err
			}
		}

		if lineNumber%IndexInterval == 0 {
			entries = append(entries, IndexEntry{
				LineNumber: lineNumber,
				Offset:     offset,
			})
		}

		offset += int64(len(line))
		lineNumber++
	}

	// Adjust offsets by the PrefixSum
	for i := range entries {
		entries[i].Offset += chunk.PrefixSum
	}

	return entries, nil
}

func loadIndex(indexFilePath string) ([]IndexEntry, error) {
	indexFile, err := os.Open(indexFilePath)
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	fileInfo, err := indexFile.Stat()
	if err != nil {
		return nil, err
	}

	entryCount := fileInfo.Size() / 16 // Each entry is 16 bytes (int64 lineNumber + int64 offset)
	indexEntries := make([]IndexEntry, 0, entryCount) // Pre-allocate the size needed for the index entries

	for {
		var lineNumber int64
		var offset int64

		err := binary.Read(indexFile, binary.LittleEndian, &lineNumber)
		if err != nil {
			if err == io.EOF {
				break
			}
			
			return nil, err
		}
		
		err = binary.Read(indexFile, binary.LittleEndian, &offset)
		if err != nil {
			return nil, err
		}

		indexEntries = append(indexEntries, IndexEntry{
			LineNumber: lineNumber,
			Offset:     offset,
		})
	}

	return indexEntries, nil
}

func findAndPrintLine(inputFilePath, indexFilePath string, targetLineNumber int64) error {
	// Read index entries into memory
	indexEntries, err := loadIndex(indexFilePath)
	if err != nil {
		return err
	}

	// Find the index entry with the largest line number <= targetLineNumber using binary search
	idx := sort.Search(len(indexEntries), func(i int) bool {
		return indexEntries[i].LineNumber > targetLineNumber
	}) - 1

	if idx < 0 {
		return fmt.Errorf("line number %d does not exist", targetLineNumber)
	}

	startLineNumber := indexEntries[idx].LineNumber
	offset := indexEntries[idx].Offset

	// Calculate the maximum bytes to read
	linesToRead := targetLineNumber - startLineNumber + 1 // Add 1 for inclusive range
	maxBytesToRead := linesToRead * MaxLineSize

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	_, err = inputFile.Seek(offset, 0)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(io.LimitReader(inputFile, maxBytesToRead))

	var lineNumber int64 = startLineNumber
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				if lineNumber == targetLineNumber {
					// Handle last line without newline
					fmt.Print(string(line))
					
					return nil
				}
				
				break
			} else {
				return err
			}
		}

		if lineNumber == targetLineNumber {
			fmt.Print(string(line))
			
			return nil
		}

		lineNumber++
	}

	return fmt.Errorf("line %d does not exist", targetLineNumber)
}
