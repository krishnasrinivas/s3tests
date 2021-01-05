package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net/url"
	"strings"

	minio "github.com/minio/minio-go"
)

func main() {
	endpointsPtr := flag.String("endpoints", "", "endpoint. ex. 192.168.1.20:9000,192.168.1.21:9000,192.168.1.22:9000")
	accessKeyPtr := flag.String("accesskey", "minioadmin", "access key")
	secretKeyPtr := flag.String("secretkey", "minioadmin", "secret key")
	bucketPtr := flag.String("bucket", "", "bucket name")
	loopPtr := flag.Int("l", 1, "loop count")
	localFilePtr := flag.String("f", "", "local file path")

	flag.Parse()

	endpoints := *endpointsPtr
	accessKey := *accessKeyPtr
	secretKey := *secretKeyPtr
	bucket := *bucketPtr
	loop := *loopPtr
	localFile := *localFilePtr

	if endpoints == "" {
		log.Fatal("endpoint not provided")
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

	endpointsSlice := strings.Split(endpoints, ",")
	var clients []*minio.Core

	for _, e := range endpointsSlice {
		u, err := url.Parse(e)
		if err != nil {
			log.Fatal("url.Parse()", err)
		}
		c, err := minio.NewCore(u.Host, accessKey, secretKey, u.Scheme == "https")
		if err != nil {
			log.Fatal("minio.NewCore()", err)
		}
		clients = append(clients, c)
	}
	object := "simple-concurrency-object"
	for l := 0; l < loop; l++ {
		for i, c := range clients {
			_, err := c.PutObject(bucket, object, bytes.NewReader(append(localFileContent, byte(i))), int64(len(localFileContent)+1), "", "", nil, nil)
			if err != nil {
				log.Fatal("c.PutObjectPart()", err)
			}
		}

		r, info, err := clients[0].GetObject(bucket, object, minio.GetObjectOptions{})
		if err != nil {
			log.Fatal("GetObject()", err)
		}
		if info.Size != int64(len(localFileContent)) {
			log.Fatal("GetObject() size not correct")
		}
		buf, err := ioutil.ReadAll(r)
		if err != nil {
			log.Fatal("ReadAll()", err)
		}
		if bytes.Compare(buf, append(localFileContent, byte(len(clients)-1))) != 0 {
			log.Fatal("contents differ")
		}
		if int64(len(buf)) != info.Size {
			log.Fatal("dataReadCount != info.Size")
		}
	}
}
