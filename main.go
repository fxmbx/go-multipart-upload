package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	BUCKET_NAME   = ""
	REGION        = "eu-west-2"
	AWS_S3_KEY    = ""
	AWS_S3_SECRET = ""
	FILE          = "test.mp4"
	PART_SIZE     = 5_000_000 // min of 5_000_000
	RETIRES       = 2
)

var s3session *s3.S3

func init() {
	s3session = s3.New(session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(REGION),
		Credentials: credentials.NewStaticCredentials(AWS_S3_KEY, AWS_S3_SECRET, ""),
	})))
}

func main() {
	file, _ := os.Open(FILE)
	defer file.Close()

	stats, err := file.Stat()
	if err != nil {
		fmt.Println(err)
	}
	fileSize := stats.Size()
	//read file into bytes array
	buffer := make([]byte, fileSize)
	file.Read(buffer)

	expiryDate := time.Now().AddDate(0, 0, 1)

	fmt.Println(file.Name())
	createdRes, err := s3session.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:  aws.String(BUCKET_NAME),
		Key:     aws.String(file.Name()),
		Expires: &expiryDate,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	var start, currentSize int
	var remaining = int(fileSize)
	var partNum = 1
	var completedParts []*s3.CompletedPart

	// loop till remaining size = 0
	for start = 0; remaining != 0; start += PART_SIZE {
		if remaining < PART_SIZE {
			currentSize = remaining
		} else {
			currentSize = PART_SIZE
		}
		completed, err := Upload(createdRes, buffer[start:start+currentSize], partNum)
		if err != nil {
			_, err := s3session.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
				Bucket:   createdRes.Bucket,
				Key:      createdRes.Key,
				UploadId: createdRes.UploadId,
			})
			if err != nil {
				fmt.Println(err)
				return
			}
		}
		remaining -= currentSize
		fmt.Printf("Part %v complete, %v bytes remaining\n", partNum, remaining)
		completedParts = append(completedParts, completed)
		partNum++
	}
	resp, err := s3session.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   createdRes.Bucket,
		Key:      createdRes.Key,
		UploadId: createdRes.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(resp.String())
	}

}

func Upload(resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNum int) (*s3.CompletedPart, error) {
	tries := 0
	for tries <= RETIRES {
		uploadRes, err := s3session.UploadPart(&s3.UploadPartInput{
			Body:          bytes.NewReader(fileBytes),
			Bucket:        resp.Bucket,
			Key:           resp.Key,
			PartNumber:    aws.Int64(int64(partNum)),
			UploadId:      resp.UploadId,
			ContentLength: aws.Int64(int64(len(fileBytes))),
		})
		if err != nil {
			fmt.Println(err)
			if tries == RETIRES {
				return nil, err
			} else {
				tries++
				time.Sleep(time.Second + 5)
			}
		} else {
			return &s3.CompletedPart{
				ETag:       uploadRes.ETag,
				PartNumber: aws.Int64(int64(partNum)),
			}, nil
		}
	}
	return nil, errors.New("something went wrong during uploading")
}
