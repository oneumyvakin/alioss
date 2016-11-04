package alioss

import (
    "testing"
    "path"
    "os"
    "log"
    "time"
    "math/rand"
    "fmt"
    "github.com/kardianos/osext"
    "path/filepath"
    "github.com/aliyun/aliyun-oss-go-sdk/oss"
)

var AliRegion string
var AliBucket string
var AliKeyId string
var AliSecretKey string

func init() {
    AliRegion = os.Getenv("ALI_REGION")
    AliBucket = os.Getenv("ALI_BUCKET")
    AliKeyId = os.Getenv("ALI_ACCESS_KEY_ID")
    AliSecretKey = os.Getenv("ALI_SECRET_ACCESS_KEY")

}

func TestResumeUpload(t *testing.T) {
    if AliRegion == "" || AliBucket == "" || AliKeyId == "" || AliSecretKey == "" {
		t.Fatal("Environment variables ALI_REGION or ALI_BUCKET or ALI_ACCESS_KEY_ID or ALI_SECRET_ACCESS_KEY are not defined")
	}

	ali, err := oss.New(AliRegion, AliKeyId, AliSecretKey)
	if err != nil {
		t.Fatalf("Failed to create OSS client: %s", err)
	}

	aliSvc := AliOss{
		Log:    log.New(os.Stdout, "testing: ", log.LstdFlags),
		Svc:    ali,
		Region: AliRegion,
		Bucket: AliBucket,
	}
    
    bucket, err := aliSvc.Svc.Bucket(aliSvc.Bucket)
    if err != nil {
		t.Fatalf("Failed to instant bucket: %s", err)
	}
    
    testFile := createTestFile(2 * DefaultUploadPartSize)
    
    imur, err := bucket.InitiateMultipartUpload(filepath.Base(testFile))
    
    unfUploads, err := aliSvc.ListUnfinishedUploads()
    if err != nil {
		t.Fatalf("Failed to list unfinished uploads: %s", err)
	}
    unfUploadFound := false
    for _, upload := range unfUploads {
        t.Log(upload.Key, upload.UploadID)
        if upload.Key == filepath.Base(testFile) {
            unfUploadFound = true
        }
    }
    if !unfUploadFound {
        t.Fatalf("Failed to find unfinished upload %s to %s", testFile, filepath.Base(testFile))
    }
    
    err = aliSvc.ResumeUpload(testFile, filepath.Base(testFile), imur.UploadID)
    if err != nil {
		t.Fatalf("Failed to resume upload %s to %s: %s", testFile, filepath.Base(testFile), err)
	}
    
    err = aliSvc.Delete(filepath.Base(testFile))
	if err != nil {
		t.Fatalf("Failed to delete file %s: %s", filepath.Base(testFile), err)
	}
}

func TestBasic(t *testing.T) {
	if AliRegion == "" || AliBucket == "" || AliKeyId == "" || AliSecretKey == "" {
		t.Fatal("Environment variables ALI_REGION or ALI_BUCKET or ALI_ACCESS_KEY_ID or ALI_SECRET_ACCESS_KEY are not defined")
	}

	ali, err := oss.New(AliRegion, AliKeyId, AliSecretKey)
	if err != nil {
		t.Fatalf("Failed to create OSS client: %s", err)
	}

	aliSvc := AliOss{
		Log:    log.New(os.Stdout, "testing: ", log.LstdFlags),
		Svc:    ali,
		Region: AliRegion,
		Bucket: AliBucket,
	}

	err = aliSvc.IsRegionValid(AliRegion)
	if err != nil {
		return
	}

	bucketList, err := aliSvc.GetBucketFilesList("")
	if err != nil {
		t.Fatalf("Failed to get bucket list: %s", err)
		return
	}
	for _, rFile := range bucketList {
		aliSvc.Log.Println(rFile.Type, rFile.Key)
	}

    subFolder := "testFolder"
    
	if subFolder != "" {
		err = aliSvc.CreateFolder(subFolder)
		if err != nil {
			t.Fatalf("Failed to create folder %s: %s", subFolder, err)
			return
		}
	}
	testFile := createTestFile(1024)
	testFileDownloaded := testFile + ".downloaded"
	testFileUploadedSuccess := false

	err = aliSvc.Upload(testFile, subFolder)
	if err != nil {
		t.Fatalf("Failed to upload file %s to %s: %s", testFile, subFolder+filepath.Base(testFile), err)

	}

	for i := 0; i <= 5; i++ {
		bucketList, err = aliSvc.GetBucketFilesList(subFolder)
		if err != nil {
			t.Fatalf("Failed to get bucket list: %s", err)

		}
		for _, rFile := range bucketList {
			aliSvc.Log.Println(rFile.Type, rFile.Key)
			if rFile.Key == subFolder+"/"+filepath.Base(testFile) {
				testFileUploadedSuccess = true
			}
		}
		if testFileUploadedSuccess {
			break
		}
	}

	if !testFileUploadedSuccess {
		t.Fatalf("Failed to upload file %s to %s: File does not exists on remote storage", testFile, subFolder+"/"+filepath.Base(testFile))
	}
    
    err = os.Remove(testFile)
    if err != nil {
        t.Fatalf("Failed to remove uploaded file %s: %s", testFile, err)
    }

	err = aliSvc.Download(subFolder+"/"+filepath.Base(testFile), testFileDownloaded)
	if err != nil {
		t.Fatalf("Failed to download file %s to %s: %s", subFolder+filepath.Base(testFile), testFileDownloaded, err)

	}
	if _, err := os.Stat(testFileDownloaded); os.IsNotExist(err) {
		t.Fatalf("Failed to download file %s to %s: File does not exists", subFolder+"/"+filepath.Base(testFile), testFileDownloaded)

	}
	err = os.Remove(testFileDownloaded)
    if err != nil {
        t.Fatalf("Failed to remove downloaded file %s: %s", testFileDownloaded, err)
    }

	err = aliSvc.Delete(subFolder + "/" + filepath.Base(testFile))
	if err != nil {
		t.Fatalf("Failed to delete file %s: %s", subFolder+filepath.Base(testFile), err)
	}
}

func createTestFile(size int64) string {
    binaryDir, err := osext.ExecutableFolder()
	if err != nil {
		panic(fmt.Errorf("Failed to get binary folder: %s", err))
	}
	testFilePath := path.Join(binaryDir, "test_"+getRandomString(10)+".txt")
	testFile, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
	if err != nil {
		panic(fmt.Errorf("Failed to create test file %s: %s", testFilePath, err))
	}
	defer func() { _ = testFile.Close() }()

    signature := []byte("test upload file")
    sigLen := 2 * len(signature) // We will write signature twice
    size = size - int64(sigLen)
    
	_, err = testFile.Write(signature)
	if err != nil {
		panic(fmt.Errorf("Failed to write first signature to test file %s: %s", testFilePath, err))
	}

    chunkSize := int64(1024 * 1024 * 25)

	for {
		if size <= chunkSize {
			s := make([]byte, size)
			_, err := testFile.Write(s)
            if err != nil {
                panic(fmt.Errorf("Failed to write last chunk to test file %s: %s", testFilePath, err))
            }
            
            _, err = testFile.Write(signature)
            if err != nil {
                panic(fmt.Errorf("Failed to write second signature to test file %s: %s", testFilePath, err))
            }

			return testFilePath
		}

		size = size - chunkSize

		s := make([]byte, chunkSize)

		_, err := testFile.Write(s)
		if err != nil {
			panic(fmt.Errorf("Failed to write test file %s: %s", testFilePath, err))
		}
	}
}

func getRandomString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)

	var src = rand.NewSource(time.Now().UnixNano())

	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}