package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"path"

	minio "github.com/minio/minio-go"
)

func main() {
	endpoint1Ptr := flag.String("endpoint1", "", "endpoint. ex. https://192.168.1.20:9000")
	endpoint6Ptr := flag.String("endpoint6", "", "endpoint. ex. https://192.168.1.20:9000")
	accessKeyPtr := flag.String("accesskey", "minioadmin", "access key")
	secretKeyPtr := flag.String("secretkey", "minioadmin", "secret key")
	bucketPtr := flag.String("bucket", "", "bucket name")
	prefixPtr := flag.String("prefix", "", "prefix")
	localFilePtr := flag.String("f", "", "local file path")

	flag.Parse()

	endpoint1 := *endpoint1Ptr
	endpoint6 := *endpoint6Ptr
	accessKey := *accessKeyPtr
	secretKey := *secretKeyPtr
	bucket := *bucketPtr
	prefix := *prefixPtr
	localFile := *localFilePtr

	if endpoint1 == "" {
		log.Fatal("endpoint1 not provided")
	}
	if endpoint6 == "" {
		log.Fatal("endpoint6 not provided")
	}
	if bucket == "" {
		log.Fatal("bucket not provided")
	}
	if localFile == "" {
		log.Fatal("local file not provided")
	}

	localFileContent, err := ioutil.ReadFile(localFile)
	if err != nil {
		log.Fatal(err)
	}

	u, err := url.Parse(endpoint1)
	if err != nil {
		log.Fatal("url.Parse()", err)
	}
	s3Client1, err := minio.New(u.Host, accessKey, secretKey, u.Scheme == "https")
	if err != nil {
		log.Fatal(err)
	}

	u, err = url.Parse(endpoint6)
	if err != nil {
		log.Fatal("url.Parse()", err)
	}
	s3Client6, err := minio.New(u.Host, accessKey, secretKey, u.Scheme == "https")
	if err != nil {
		log.Fatal(err)
	}

	objName := localFile
	if prefix != "" {
		objName = path.Join(prefix, localFile)
	}
	_, err = s3Client1.PutObject(bucket, objName, bytes.NewBuffer(localFileContent), int64(len(localFileContent)), minio.PutObjectOptions{})
	if err != nil {
		log.Fatal(err)
	}

	localFileContent = append(localFileContent, 'a')
	_, err = s3Client6.PutObject(bucket, objName, bytes.NewBuffer(localFileContent), int64(len(localFileContent)), minio.PutObjectOptions{})
	if err != nil {
		log.Fatal(err)
	}

	obj, err := s3Client1.GetObject(bucket, objName, minio.GetObjectOptions{})
	if err != nil {
		log.Fatal(err)
	}
	objContent, err := ioutil.ReadAll(obj)
	if err != nil {
		log.Fatal(err)
	}
	if bytes.Compare(objContent, localFileContent) != 0 {
		fmt.Println("contents differ for", objName)
	}
}
