// Package alioss is a simple wrapper around Aliyun OSS service
package alioss

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// Main entry point for service manipulation
type AliOss struct {
	Log    *log.Logger
	Svc    *oss.Client
	Region string
	Bucket string
}

type downloader struct {
	AliOss

	File       *os.File
	FileOffset int64
	Err        error
}

type filePart struct {
	Key        string
	Range      string
	Etag       string
	Offset     int64
	Length     int64
	PartNumber int
	Body       []byte
}

// Get regions(endpoints)
func (alioss AliOss) GetRegions() []string {
	var regions = []string{
		"oss-cn-hangzhou.aliyuncs.com",
		"oss-cn-shanghai.aliyuncs.com",
		"oss-cn-qingdao.aliyuncs.com",
		"oss-cn-beijing.aliyuncs.com",
		"oss-cn-shenzhen.aliyuncs.com",
		"oss-cn-hongkong.aliyuncs.com",
		"oss-us-west-1.aliyuncs.com",
		"oss-us-east-1.aliyuncs.com",
		"oss-ap-southeast-1.aliyuncs.com",
	}
	return regions
}

// Check region(endpoint) name
func (alioss AliOss) IsRegionValid(name string) error {
	regions := alioss.GetRegions()
	sort.Strings(regions)
	i := sort.SearchStrings(regions, name)
	if i < len(regions) && regions[i] == name {
		alioss.Log.Println("Region valid:", name)
		return nil
	}

	return fmt.Errorf("Failed to validate region: %s", name)
}

// List available buckets
func (alioss AliOss) GetBucketsList() (list []string, err error) {
	result, err := alioss.Svc.ListBuckets()
	if err != nil {
		alioss.Log.Printf("Failed to list buckets: %s\n", err)
		return
	}

	for _, bucket := range result.Buckets {
		list = append(list, bucket.Name)
	}

	alioss.Log.Println("Get buckets:", list)
	return
}

// Create bucket if doesn't exists
func (alioss AliOss) CreateBucket(name string) error {
	buckets, err := alioss.GetBucketsList()
	if err != nil {
		return err
	}

	sort.Strings(buckets)
	i := sort.SearchStrings(buckets, name)
	if i < len(buckets) && buckets[i] == name {
		alioss.Log.Println("Bucket already exists:", name)
		return nil
	}

	err = alioss.Svc.CreateBucket(name)
	if err != nil {
		alioss.Log.Printf("Failed to create bucket %s: %s", name, err)
		return err
	}

	alioss.Log.Println("Create bucket:", name)
	return nil
}

// Create folder
func (alioss AliOss) CreateFolder(path string) error {
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed to create folder %s: %s\n", path, err)
		return err
	}
	var emptyReader io.Reader
	err = bucket.PutObject(path+"/", emptyReader)
	if err != nil {
		alioss.Log.Printf("Failed to create folder %s: %s\n", path, err)
		return err
	}

	return err
}

// List files and folders.
// SubFolder can be ""
func (alioss AliOss) GetBucketFilesList(subFolder string) ([]oss.ObjectProperties, error) {
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed to list objects: %s\n", err)
		return nil, err
	}

	subFolder = strings.TrimSuffix(subFolder, "/")
	if subFolder != "" {
		subFolder = subFolder + "/"
	}
	result, err := bucket.ListObjects(oss.Prefix(subFolder), oss.Delimiter("/"))
	if err != nil {
		alioss.Log.Printf("Failed to list objects: %s\n", err)
		return nil, err
	}

	alioss.Log.Printf("Get bucket files in /%s:\n", subFolder, result.Objects)
	return result.Objects, nil
}

// Get file info
// Returns HTTP headers
func (alioss AliOss) GetFileInfo(path string) (headers http.Header, err error) {
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed to get file %s info: %s\n", path, err)
		return
	}

	isExist, err := bucket.IsObjectExist(path)
	if err != nil {
		alioss.Log.Printf("Failed to get file %s info: %s\n", path, err)
		return
	}
	if !isExist {
		alioss.Log.Printf("Failed to get file info: File does not exists: %s", path)
		return
	}

	headers, err = bucket.GetObjectDetailedMeta(path)
	if err != nil {
		alioss.Log.Printf("Failed to get file %s info: %s\n", path, err)
		return
	}

	alioss.Log.Println("Get file info:", path, headers)
	return
}

// Get file part
func (alioss AliOss) GetFilePart(path string, start int64, end int64) (resp io.ReadCloser, err error) {
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed to get file %s info: %s\n", path, err)
		return
	}

	resp, err = bucket.GetObject(path, oss.Range(start, end))
	if err != nil {
		alioss.Log.Printf("Failed to get file %s part: %s\n", path, err)
		return
	}

	alioss.Log.Println("Get file part:", path)
	return
}

// Delete file
func (alioss AliOss) Delete(path string) (err error) {
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed to get file %s info: %s\n", path, err)
		return
	}

	err = bucket.DeleteObject(path)
	if err != nil {
		alioss.Log.Println("Failed to delete:", path, err)
		return
	}

	alioss.Log.Println("Delete path:", path)
	return
}

// List bucket's unfinished uploads
func (alioss AliOss) ListUnfinishedUploads() ([]oss.UncompletedUpload, error) {
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed list unfinised uploads: %s\n", err)
		return nil, err
	}

	resp, err := bucket.ListMultipartUploads()
	if err != nil {
		alioss.Log.Printf("Failed list unfinised uploads: %s\n", err)
		return nil, err
	}

	alioss.Log.Println("List bucket's unfinished uploads", resp)
	return resp.Uploads, nil
}

// List parts of unfinished uploads
// Returns https://github.com/aliyun/aliyun-oss-go-sdk/blob/033d39afc575aa38ac40f8e2011710b7bacf9f7a/oss/type.go#L319
// Parts []UploadedParts - can be empty
// UploadedParts.PartNumber
// UploadedParts.Size
func (alioss AliOss) ListParts(key string, uploadId string) (resp oss.ListUploadedPartsResult, err error) {
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed list parts: %s\n", err)
		return
	}

	resp, err = bucket.ListUploadedParts(
		oss.InitiateMultipartUploadResult{
			Bucket:   alioss.Bucket,
			Key:      key,
			UploadID: uploadId,
		},
	)
	if err != nil {
		alioss.Log.Printf("Failed list parts: %s\n", err)
		return
	}

	alioss.Log.Printf("List parts for key %s of upload id %s: %s\n", key, uploadId, resp)
	return
}

// Abort upload
func (alioss AliOss) AbortUpload(key string, uploadId string) (err error) {
	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed abort upload: %s\n", err)
		return
	}

	err = bucket.AbortMultipartUpload(
		oss.InitiateMultipartUploadResult{
			Bucket:   alioss.Bucket,
			Key:      key,
			UploadID: uploadId,
		},
	)
	if err != nil {
		alioss.Log.Printf("Failed abort upload: %s\n", err)
		return
	}

	alioss.Log.Printf("Abort upload for key %s of upload id %s\n", key, uploadId)
	return
}

// Complete upload
func (alioss AliOss) CompleteUpload(key string, uploadId string) (err error) {
	respParts, err := alioss.ListParts(key, uploadId) // Just for debug
	if err != nil {
		alioss.Log.Printf("Failed to complete upload: Failed to list parts for key %s of upload id %s: %s\n", key, uploadId, err)
		return
	}

	bucket, err := alioss.Svc.Bucket(alioss.Bucket)
	if err != nil {
		alioss.Log.Printf("Failed complete upload: %s\n", err)
		return
	}

	var completedParts []oss.UploadPart
	for _, part := range respParts.UploadedParts {
		completedPart := oss.UploadPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		}
		completedParts = append(completedParts, completedPart)
	}
	resp, err := bucket.CompleteMultipartUpload(
		oss.InitiateMultipartUploadResult{
			Bucket:   alioss.Bucket,
			Key:      key,
			UploadID: uploadId,
		},
		completedParts,
	)
	if err != nil {
		alioss.Log.Printf("Failed to complete upload for key %s of upload id %s: %s\n", key, uploadId, err)
		return
	}
	alioss.Log.Printf("Complete upload for key %s of upload id %s: %s\n", key, uploadId, resp)

	return
}

// Close resources and log if error
func (alioss AliOss) IoClose(c io.Closer) {
	err := c.Close()
	if err != nil {
		alioss.Log.Println(err)
	}
}
