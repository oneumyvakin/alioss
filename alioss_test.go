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

func TestBasic(t *testing.T) {
    AliRegion := os.Getenv("ALI_REGION")
	AliBucket := os.Getenv("ALI_BUCKET")
	AliKeyId := os.Getenv("ALI_ACCESS_KEY_ID")
	AliSecretKey := os.Getenv("ALI_SECRET_ACCESS_KEY")

	if AliRegion == "" || AliBucket == "" || AliKeyId == "" || AliSecretKey == "" {
		t.Fatal("Environment variables ALI_REGION or ALI_BUCKET or ALI_ACCESS_KEY_ID or ALI_SECRET_ACCESS_KEY are not defined")
	}

	// Create dummy Ali instance to determine bucket region
	ali, err := oss.New(AliRegion, AliKeyId, AliSecretKey)
	if err != nil {
		t.Fatalf("Failed to create OSS client: %s", err)
	}

	aliSvc := AliOss{
		Log:    log.New(os.Stdout, "experiments: ", log.LstdFlags),
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
	testFile := createTestFile()
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


func createTestFile() string {
    binaryDir, err := osext.ExecutableFolder()
	if err != nil {
		panic(fmt.Errorf("Failed to get binary folder: %s", err))
	}
	testFilePath := path.Join(binaryDir, "test_"+getRandomString(10)+".txt")
	testFile, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal("Failed to create test file", testFilePath, err)
	}
	defer func() { _ = testFile.Close() }()

	_, err = testFile.Write([]byte("test amazon s3"))
	if err != nil {
		return ""
	}

	return testFilePath
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