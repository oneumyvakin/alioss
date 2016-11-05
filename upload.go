package alioss

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

const (
	DefaultUploadConcurrency int   = 5
	DefaultUploadPartSize    int64 = 5 * 1024 * 1024 // 5Mb
)

// Upload filePath to destinationPath, where destinationPath contains only folders like /folder/folder2
func (alioss AliOss) Upload(filePath, destinationPath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Failed to open file %s for upload: %s\n", filePath, err)
	}
	alioss.IoClose(file)

	key := destinationPath + "/" + filepath.Base(filePath)
	if destinationPath == "" || destinationPath == "/" {
		key = filepath.Base(filePath)
	}

	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		return fmt.Errorf("Failed upload file %s: %s\n", filePath, err)
	}

	err = bucket.UploadFile(key, filePath, DefaultUploadPartSize, oss.Routines(DefaultUploadConcurrency), oss.Checkpoint(true, ""))
	if err != nil {
		return fmt.Errorf("Failed upload file %s: %s\n", filePath, err)
	}

	alioss.Log.Println("Successfully uploaded to", key)
	return nil
}

// Resume upload of local "filePath" to remote "key" identified by "uploadId"
func (alioss AliOss) ResumeUpload(filePath, key, uploadId string) (err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Failed to open file %s for upload: %s\n", filePath, err)
	}

	// Not required, but you could zip the file before uploading it
	// using io.Pipe read/writer to stream gzip'd file contents.
	pipeReader, writer := io.Pipe()

	go func() {
		bw := bufio.NewWriter(writer)
		written, err := io.Copy(bw, file)
		if err != nil {
			alioss.Log.Printf("Upload buffer io.Copy error: %s\n", err)
		}
		alioss.Log.Printf("Upload buffer io.Copy written: %s\n", written)

		alioss.IoClose(file)
		err = bw.Flush()
		if err != nil {
			alioss.Log.Printf("bufio flush error: %s\n", err)
		}
		alioss.IoClose(writer)
	}()

	alioss.Log.Printf("Resume Upload %s to %s\n", filePath, key)

	resp, err := alioss.ListParts(key, uploadId)
	if err != nil {
		return fmt.Errorf("Failed to list uploaded parts for key %s of upload id %s: %s\n", key, uploadId, err)
	}

	partQueue := make(chan filePart, DefaultUploadConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < DefaultUploadConcurrency; i++ {
		wg.Add(1)
		go alioss.asyncUploadPart(key, uploadId, partQueue, &wg)
	}

	go alioss.getFileParts(partQueue, pipeReader, resp.UploadedParts)

	alioss.Log.Println("Wait for all parts are uploading...")
	wg.Wait()

	err = alioss.CompleteUpload(key, uploadId)
	if err != nil {
		return fmt.Errorf("Failed to complete upload with key %s: %s\n", key, err)
	}

	alioss.Log.Println("Successfully resumed upload to", key)

	return nil
}

func (alioss AliOss) getFileParts(partChan chan<- filePart, reader io.Reader, uploadedParts []oss.UploadedPart) {
	var offset int64
	lastPartNumber := 1
	offset = 0

	for {
		part := make([]byte, DefaultUploadPartSize)
		partSize, errRead := io.ReadFull(reader, part)
		if errRead != nil && errRead != io.EOF && errRead != io.ErrUnexpectedEOF {
			alioss.Log.Fatalf("Failed to read part number %s from reader at offset %s: %s\n", lastPartNumber, offset, errRead)
		}
		alioss.Log.Printf("Read bytes %s for part number %d with size: %s\n", partSize, lastPartNumber, len(part))

		if int64(partSize) != DefaultUploadPartSize {
			lastPart := make([]byte, partSize)
			copy(lastPart, part)
			part = lastPart
		}

		partEtag, err := alioss.getPartEtag(part)
		if err != nil {
			alioss.Log.Fatalf("Failed to get Etag for part number %s with size %s: %s\n", lastPartNumber, partSize, err)
		}

		alioss.Log.Printf("Part number %s size bytes %s has ETag: %s\n", lastPartNumber, len(part), partEtag)

		if true == alioss.needToUpload(uploadedParts, lastPartNumber, partEtag) {
			partChan <- filePart{
				Body:       part,
				PartNumber: lastPartNumber,
			}
		}

		offset = offset + int64(len(part))
		lastPartNumber = lastPartNumber + 1

		if errRead == io.EOF || errRead == io.ErrUnexpectedEOF {
			alioss.Log.Printf("EOF or ErrUnexpectedEOF. All parts are read and send to upload. Last part is %s, offset is %d", lastPartNumber, offset)
			close(partChan)
			return
		}
	}
}

func (alioss AliOss) needToUpload(uploadedParts []oss.UploadedPart, partNumber int, partEtag string) bool {
	for _, part := range uploadedParts {
		if part.PartNumber == partNumber {
			alioss.Log.Printf("Part number %s with ETag %s found\n", part.PartNumber, string(part.ETag))

			if part.ETag == partEtag {
				alioss.Log.Printf("Match Etag for part number %s with size %s ETag %s == %s.\n", part.PartNumber, part.Size, string(part.ETag), partEtag)
				return false
			} else {
				alioss.Log.Printf("Mismatch Etag for part number %s with size %s ETag %s != %s. Reuploading...\n", part.PartNumber, part.Size, string(part.ETag), partEtag)
				return true
			}
		}
	}
	alioss.Log.Printf("Part number %s not found\n", partNumber)

	return true
}

func (alioss AliOss) asyncUploadPart(key string, uploadId string, partChan <-chan filePart, wg *sync.WaitGroup) {
	defer wg.Done()
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed to upload part for key %s: %s\n", key, err)
		return
	}

	for {
		if part, ok := <-partChan; ok {
			alioss.Log.Printf("Start to upload part number %s for key %s\n", part.PartNumber, key)
			_, err := bucket.UploadPart(
				oss.InitiateMultipartUploadResult{
					Bucket:   alioss.Bucket,
					Key:      key,
					UploadID: uploadId,
				},
				bytes.NewReader(part.Body),
				DefaultUploadPartSize,
				part.PartNumber,
			)
			if err != nil {
				alioss.Log.Printf("Failed to upload part number %s for key %s: %s\n", part.PartNumber, key, err)
				return
			}
			alioss.Log.Printf("Finished upload part number %s for key %s\n", part.PartNumber, key)
		} else {
			alioss.Log.Println("Upload channel closed. Return.")

			return
		}
	}

	return
}

func (alioss AliOss) uploadPart(key string, partNumber int, uploadId string, body []byte) (err error) {
	alioss.Log.Printf("Start upload part number %d of key %s for upload id %s\n", partNumber, key, uploadId)

	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed to upload part for key %s: %s\n", key, err)
		return
	}

	_, err = bucket.UploadPart(
		oss.InitiateMultipartUploadResult{
			Bucket:   alioss.Bucket,
			Key:      key,
			UploadID: uploadId,
		},
		bytes.NewReader(body),
		DefaultUploadPartSize,
		partNumber,
	)

	return
}

func (alioss AliOss) getPartEtag(part []byte) (etag string, err error) {
	hasher := md5.New()
	_, err = hasher.Write(part)
	if err != nil {
		alioss.Log.Printf("Failed to write part to hasher: %s", err)
		return
	}
	etag = fmt.Sprintf("\"%s\"", hex.EncodeToString(hasher.Sum(nil)))

	return
}
