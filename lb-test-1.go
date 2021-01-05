package main

import (
	"bytes"
	"crypto/rand"
	"flag"
	"io"
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

	flag.Parse()

	endpoints := *endpointsPtr
	accessKey := *accessKeyPtr
	secretKey := *secretKeyPtr
	bucket := *bucketPtr

	if endpoints == "" {
		log.Fatal("endpoint not provided")
	}
	if bucket == "" {
		log.Fatal("bucket not provided")
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
	object := "mpobject"
	id, err := clients[0].NewMultipartUpload(bucket, object, minio.PutObjectOptions{})
	if err != nil {
		log.Fatal("NewMultipartUpload()", err)
	}
	partData := make([]byte, 5*1024*1024)
	_, err = rand.Read(partData)
	if err != nil {
		log.Fatal("rand.Read()", err)
	}
	var parts []minio.ObjectPart
	partNum := 1
	for {
		for _, c := range clients {
			if partNum == 101 {
				break
			}
			part, err := c.PutObjectPart(bucket, object, id, partNum, bytes.NewReader(partData), int64(len(partData)), "", "", nil)
			if err != nil {
				log.Fatal("c.PutObjectPart()", err)
			}
			parts = append(parts, part)
			partNum++
		}
	}
	var completeParts []minio.CompletePart
	for _, part := range parts {
		completeParts = append(completeParts, minio.CompletePart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}
	_, err = clients[0].CompleteMultipartUpload(bucket, object, id, completeParts)
	if err != nil {
		log.Fatal("CompleteMultipartUpload()", err)
	}

	r, info, err := clients[len(clients)-1].GetObject(bucket, object, minio.GetObjectOptions{})
	if err != nil {
		log.Fatal("GetObject()", err)
	}
	if info.Size != int64(100*len(partData)) {
		log.Fatal("GetObject() size not correct")
	}
	buf := make([]byte, len(partData))
	dataReadCount := 0
	for {
		n, err := r.Read(buf)
		if err == nil {
			dataReadCount += n
			if bytes.Compare(buf, partData) != 0 {
				if err != nil {
					log.Fatal("contents differ")
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("r.Read()", err)
		}
	}
	if int64(dataReadCount) != info.Size {
		log.Fatal("dataReadCount != info.Size")
	}
}
