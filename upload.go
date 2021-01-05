package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"path"
	"strconv"
	"sync"

	minio "github.com/minio/minio-go"
)

func main() {
	endpointPtr := flag.String("endpoint", "", "endpoint. ex. https://192.168.1.20:9000")
	accessKeyPtr := flag.String("accesskey", "minioadmin", "access key")
	secretKeyPtr := flag.String("secretkey", "minioadmin", "secret key")
	bucketPtr := flag.String("bucket", "", "bucket name")
	prefixPtr := flag.String("prefix", "", "prefix")
	concurrencyPtr := flag.Int("t", 1, "threads")
	numFilesPtr := flag.Int("n", 0, "number of files")
	localFilePtr := flag.String("f", "", "local file path")

	flag.Parse()

	endpoint := *endpointPtr
	accessKey := *accessKeyPtr
	secretKey := *secretKeyPtr
	bucket := *bucketPtr
	prefix := *prefixPtr
	concurrency := *concurrencyPtr
	numFiles := *numFilesPtr
	localFile := *localFilePtr

	if endpoint == "" {
		log.Fatal("endpoint not provided")
	}
	if bucket == "" {
		log.Fatal("bucket not provided")
	}
	if numFiles == 0 {
		log.Fatal("number of files not provided")
	}
	if localFile == "" {
		log.Fatal("local file not provided")
	}

	localFileContent, err := ioutil.ReadFile(localFile)
	if err != nil {
		log.Fatal(err)
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatal("url.Parse()", err)
	}
	s3Client, err := minio.New(u.Host, accessKey, secretKey, u.Scheme == "https")
	if err != nil {
		log.Fatal(err)
	}

	q := make(chan struct{}, concurrency)

	fileNum := 0
	var wg sync.WaitGroup
	for {
		if fileNum == numFiles {
			break
		}
		q <- struct{}{}
		wg.Add(1)
		go func(n int) {
			defer func() { <-q }()
			defer wg.Done()
			nStr := strconv.Itoa(n)
			buf := bytes.Buffer{}
			buf.Write(localFileContent)
			buf.Write([]byte(nStr))
			objName := fmt.Sprintf("%s.%s", localFile, nStr)
			if prefix != "" {
				objName = path.Join(prefix, nStr)
			}
			_, err = s3Client.PutObject(bucket, objName, &buf, int64(buf.Len()), minio.PutObjectOptions{})
			if err != nil {
				log.Fatal(err)
			}
		}(fileNum)
		fileNum++
	}
	wg.Wait()
}
