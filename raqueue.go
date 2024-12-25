package raqueue

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type RAQueue struct {
	msgIndex uint   // Tracks the next message index to read
	totalMsg uint   // Total number of messages in the queue
	capacity uint   // Maximum capacity of the queue
	filepath string // Path to the file storing the queue
	offset   int64  // Tracks the byte offset for consecutive reads
}

type Option struct {
	Capacity uint
}

var raQueueClient *RAQueue

// New initializes the RAQueue instance
func New(option *Option) *RAQueue {
	if raQueueClient == nil {
		filePath, err := createFile()
		if err != nil {
			panic(err)
		}
		raQueueClient = &RAQueue{
			msgIndex: 0,
			totalMsg: 0,
			capacity: max(1, option.Capacity),
			filepath: filePath,
			offset:   0,
		}
	}
	return raQueueClient
}

// Send appends data to the queue
func (r *RAQueue) Send(v interface{}) (bool, error) {
	// Serialize data to JSON
	data, err := json.Marshal(v)
	if err != nil {
		return false, err
	}

	// Open file in append mode
	file, err := os.OpenFile(r.filepath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return false, err
	}
	defer file.Close()

	// Write data with a newline delimiter
	if _, err := file.Write(append(data, '\n')); err != nil {
		return false, err
	}

	r.totalMsg++
	r.logAction("Send")
	return true, nil
}

// Read reads the next message from the queue
func (r *RAQueue) Read(v interface{}) error {
	// Open file in read mode
	file, err := os.Open(r.filepath)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer file.Close()

	// Seek to the last read position
	if _, err := file.Seek(r.offset, 0); err != nil {
		return fmt.Errorf("could not seek to offset: %v", err)
	}

	// Read the next line
	reader := bufio.NewReader(file)
	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("could not read line: %v", err)
	}

	// Update the offset
	r.offset += int64(len(line))

	// Deserialize JSON data
	if err := json.Unmarshal([]byte(line), v); err != nil {
		return fmt.Errorf("could not unmarshal data: %v", err)
	}

	r.msgIndex++
	r.logAction("Read")
	return nil
}

// logAction logs the actions performed on the queue
func (r *RAQueue) logAction(action string) {
	fmt.Printf("[%s] Action: %s, File: %s\n", time.Now().Format(time.RFC3339), action, r.filepath)
}

// createFile creates a new file for the queue
func createFile() (string, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("could not get current directory: %v", err)
	}

	dirPath := currentDir + "/raqueue_data"
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return "", fmt.Errorf("could not create directory: %v", err)
	}

	filePath := fmt.Sprintf("%s/raqueue_%d.txt", dirPath, time.Now().UnixNano())
	file, err := os.Create(filePath)
	if err != nil {
		return "", fmt.Errorf("could not create file: %v", err)
	}
	defer file.Close()

	fmt.Println("File created successfully:", filePath)
	return filePath, nil
}

func max(a, b uint) uint {
	if a > b {
		return a
	}
	return b
}
