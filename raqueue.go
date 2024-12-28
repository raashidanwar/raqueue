package raqueue

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type RAQueue struct {
	msgIndex uint       // Tracks the next message index to read
	totalMsg uint       // Total number of messages in the queue
	filepath string     // Path to the file storing the queue
	offset   int64      // Tracks the byte offset for consecutive reads
	m        sync.Mutex // Mutex for concurrent access
}

var raQueueClient *RAQueue

func New() *RAQueue {
	if raQueueClient == nil {
		filePath, err := createFile()
		if err != nil {
			panic(err)
		}
		raQueueClient = &RAQueue{
			msgIndex: 0,
			totalMsg: 0,
			filepath: filePath,
			offset:   0,
		}
	}
	return raQueueClient
}

func (r *RAQueue) Send(v interface{}) (bool, error) {
	r.m.Lock()
	defer r.m.Unlock()
	data, err := json.Marshal(v)
	if err != nil {
		return false, err
	}

	file, err := os.OpenFile(r.filepath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return false, err
	}
	defer file.Close()

	if _, err := file.Write(append(data, '\n')); err != nil {
		return false, err
	}

	r.totalMsg++
	r.logAction("Send")
	return true, nil
}

func (r *RAQueue) Read(v interface{}) error {
	r.m.Lock()
	defer r.m.Unlock()
	file, err := os.Open(r.filepath)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer file.Close()

	if _, err := file.Seek(r.offset, 0); err != nil {
		return fmt.Errorf("could not seek to offset: %v", err)
	}

	reader := bufio.NewReader(file)
	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("could not read line: %v", err)
	}

	r.offset += int64(len(line))

	if err := json.Unmarshal([]byte(line), v); err != nil {
		return fmt.Errorf("could not unmarshal data: %v", err)
	}

	r.msgIndex++
	r.logAction("Read")
	return nil
}

func (r *RAQueue) logAction(action string) {
	fmt.Printf("[%s] Action: %s, File: %s\n", time.Now().Format(time.RFC3339), action, r.filepath)
}

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
