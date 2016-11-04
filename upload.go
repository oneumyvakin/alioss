package alioss

import (
    "fmt"
    "os"
    "path/filepath"
    
    "github.com/aliyun/aliyun-oss-go-sdk/oss"
)

const (
    DefaultUploadConcurrency int = 5
    DefaultUploadPartSize int64 = 5 * 1024 * 1024 // 5Mb
)

// Upload filePath to destinationPath, where destinationPath contains only folders like /folder/folder2
func (alioss AliOss) Upload(filePath, destinationPath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Failed to open file %s for upload: %s\n", filePath, err)
	}
    alioss.IoClose(file)

	key := destinationPath + "/" + filepath.Base(filePath)

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

