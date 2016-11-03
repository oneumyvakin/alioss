package alioss

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	DefaultDownloadConcurrency int   = 5
	DefaultDownloadPartSize    int64 = 5 * 1024 * 1024 // 5Mb

)

// Optimistic download
func (alioss AliOss) Download(fileName, destinationPath string) error {
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		return fmt.Errorf("Failed to download file %s: %s\n", fileName, err)
	}

	err = bucket.GetObjectToFile(fileName, destinationPath)
	if err != nil {
		return fmt.Errorf("Failed to download file %s to %s: %s\n", fileName, destinationPath, err)
	}

	return nil
}

// Resume optimistic download
func (alioss AliOss) ResumeDownload(fileName, destinationPath string) error {
	remoteFileInfo, err := alioss.GetFileInfo(fileName)
	if err != nil {
		alioss.Log.Printf("Failed to get file %s: %s\n", fileName, err)
		return err
	}

	file, err := os.OpenFile(destinationPath, os.O_WRONLY, 666)
	if err != nil {
		return fmt.Errorf("Failed to create destination file %s: %s\n", destinationPath, err)
	}
	defer alioss.IoClose(file)

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("Failed to stat destination file %s: %s\n", destinationPath, err)
	}
	var contentLength int64
	contentLength, err = strconv.Atoi(remoteFileInfo.Get("Content-Length"))
	if err != nil {
		return fmt.Errorf("Failed to get header Content-Length of remote file %s: %s\n", fileName, err)
	}
	if contentLength < stat.Size() {
		return fmt.Errorf("Failed to compare size of remote %s and destination file %s: %d <= %d\n", fileName, destinationPath, contentLength, stat.Size())
	}

	if contentLength == stat.Size() {
		alioss.Log.Printf("Size of remote %s and destination %s file match: %d == %d. Nothing to do.\n", fileName, destinationPath, contentLength, stat.Size())
		return nil
	}

	d := downloader{
		AliOss:     alioss,
		File:       file,
		FileOffset: stat.Size(),
	}

	taskPartChan := make(chan filePart, DefaultDownloadConcurrency)
	var wg sync.WaitGroup
	for i := 0; i < DefaultDownloadConcurrency; i++ {
		wg.Add(1)
		go d.asyncDownloadPart(taskPartChan, &wg)
	}

	partOffset := stat.Size()
	leftBytes := contentLength - stat.Size()
	go func() {
		for {
			alioss.Log.Printf("Resume download: Left bytes %d\n", leftBytes)
			if leftBytes <= DefaultDownloadPartSize {
				partRange := fmt.Sprintf("bytes=%d-%d", partOffset, partOffset+leftBytes-1)
				alioss.Log.Printf("Resume download: File range %s\n", partRange)
				taskPartChan <- filePart{
					Key:    fileName,
					Range:  partRange,
					Offset: partOffset,
					Length: leftBytes,
					Body:   make([]byte, leftBytes),
				}
				close(taskPartChan)
				alioss.Log.Println("Resume download: All parts send to download. Close channel.")
				return
			}
			fileRange := fmt.Sprintf("bytes=%d-%d", partOffset, partOffset+DefaultDownloadPartSize-1)
			alioss.Log.Printf("Resume download: Part range %s\n", fileRange)
			alioss.Log.Printf("Resume download: Part offset %d\n", partOffset)

			taskPartChan <- filePart{
				Key:    fileName,
				Range:  fileRange,
				Offset: partOffset,
				Length: DefaultDownloadPartSize,
			}
			partOffset = partOffset + DefaultDownloadPartSize
			leftBytes = leftBytes - DefaultDownloadPartSize
		}
	}()

	wg.Wait()

	return nil
}

func (alioss *downloader) asyncDownloadPart(taskPartChan <-chan filePart, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if part, ok := <-taskPartChan; ok {
			if alioss.Err != nil {
				alioss.Log.Printf("Failed to start download: %s\n", alioss.Err)
				return
			}
			alioss.Log.Printf("Start to download part for key %s: Range: %s, Offset: %d, Length: %d\n", part.Key, part.Range, part.Offset, part.Length)

			body, err := alioss.GetFilePart(part.Key, part.Offset, part.Offset+part.Length-1)
			alioss.Log.Printf("Request sent for %s range %s\n", part.Key, part.Range)
			if err != nil {
				alioss.Log.Printf("Failed to download file %s range %s: %s\n", part.Key, part.Range, err)
				return
			}
			alioss.Log.Printf("Recieved response for %s range %s\n", part.Key, part.Range)
			alioss.Log.Printf("File offset: %d\n", alioss.FileOffset)
			alioss.Log.Printf("Part offset: %d\n", part.Offset)
			//defer alioss.IoClose(body)

			for {
				if alioss.FileOffset == part.Offset {
					n, err := io.Copy(alioss.File, body)
					if err != nil {
						alioss.Err = err
						alioss.Log.Printf("Failed to write file %s range %s: %s\n", part.Key, part.Range, err)
						alioss.IoClose(body)
						return
					}
					alioss.IoClose(body)
					alioss.Log.Printf("Finish write %d bytes part range %s for key %s \n", n, part.Range, part.Key)

					alioss.FileOffset = part.Offset + part.Length
					alioss.Log.Printf("New file offset: %d\n", alioss.FileOffset)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		} else {
			alioss.Log.Println("Download channel closed. Return.")

			return
		}
	}
}
