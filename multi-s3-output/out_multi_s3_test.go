/**
 * (C) Copyright IBM Corp. 2020.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/minio/minio-go"
	"github.com/stretchr/testify/assert"
	"github.com/ugorji/go/codec"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"testing"
	"time"
)

type ArchiveLogEntry struct {
	Timestamp time.Time
	Log       string
}

func TestArchiveLog(t *testing.T) {
	var buf bytes.Buffer
	var handle codec.MsgpackHandle
	enc := codec.NewEncoder(&buf, &handle)

	// 2020-08-31T07:24:26.064569067Z stdout F Added `storage` successfully.
	ts, err := time.Parse(time.RFC3339, "2020-08-31T07:24:26Z")
	assert.Nil(t, err)
	unixTs := ts.Unix()
	log := "Added \"storage\" successfully."
	entry := make(map[string]interface{})
	entry["stream"] = []byte("stdout")
	entry["log"] = []byte(log)
	record := []interface{}{unixTs, entry}

	err = enc.Encode(record)
	assert.Nil(t, err)

	var archiveBuf bytes.Buffer
	data := buf.Bytes()
	result := ArchiveLog(data, &archiveBuf)
	assert.Equal(t, output.FLB_OK, result)
	gr, err := gzip.NewReader(&archiveBuf)
	assert.Nil(t, err)
	archiveData, err := ioutil.ReadAll(gr)
	assert.Nil(t, err)

	var archiveLogEntry ArchiveLogEntry
	err = json.Unmarshal(archiveData, &archiveLogEntry)
	assert.Nil(t, err)
	assert.Equal(t, ts.Unix(), archiveLogEntry.Timestamp.Unix())
	assert.Equal(t, log, archiveLogEntry.Log)
}

func TestPutLogObject(t *testing.T) {
	ctx := S3BucketContext{
		Endpoint:       "s3.us-south.cloud-object-storage.appdomain.cloud",
		EndpointSchema: "https://",
		Bucket:         "fluent-bit-multi-s3-tests",
		AccessKey:      os.Getenv("COS_AccessKey"),
		SecretKey:      os.Getenv("COS_SecretKey"),
	}
	objectKey := "testsObject.log.gz"

	minioClient, err := createMinioClient(ctx.Endpoint, ctx.EndpointSchema, ctx.AccessKey, ctx.SecretKey)
	assert.Nil(t, err)

	_ = minioClient.RemoveObject(ctx.Bucket, objectKey) // ensure there is no object in the bucket

	content1 := []byte("{\"timestamp\":\"2020-08-31T07:24:26Z\", \"log\": \"message1\"}\n")
	reader1, size1, err := gzipBytes(content1)
	assert.Nil(t, err)

	err = PutLogObject(ctx, objectKey, reader1, int64(size1))
	assert.Nil(t, err, getUrlError(err))

	object, err := minioClient.GetObject(ctx.Bucket, objectKey, minio.GetObjectOptions{})
	assert.Nil(t, err)

	buf := new(bytes.Buffer)
	gr, err := gzip.NewReader(object)
	assert.Nil(t, err)
	_, err = io.Copy(buf, gr)
	assert.Nil(t, err)

	assert.Equal(t, string(content1), string(buf.Bytes()))

	content2 := []byte("{\"timestamp\":\"2020-08-31T07:25:35Z\", \"log\": \"message2\"}\n")
	reader2, size2, err := gzipBytes(content2)
	assert.Nil(t, err)

	err = PutLogObject(ctx, objectKey, reader2, int64(size2))
	assert.Nil(t, err, getUrlError(err))

	object, err = minioClient.GetObject(ctx.Bucket, objectKey, minio.GetObjectOptions{})
	assert.Nil(t, err)

	buf = new(bytes.Buffer)
	gr, err = gzip.NewReader(object)
	assert.Nil(t, err)
	_, err = io.Copy(buf, gr)
	assert.Nil(t, err)

	assert.Equal(t, string(append(content1, content2...)), string(buf.Bytes()))
}

func gzipBytes(content []byte) (compressed io.Reader, size int, err error) {
	buf := new(bytes.Buffer)
	gw := gzip.NewWriter(buf)
	_, err = gw.Write(content)
	if err != nil {
		return
	}
	err = gw.Close()
	if err != nil {
		return
	}
	compressed = buf
	size = buf.Len()
	return
}

func getUrlError(err error) error {
	if urlErr, ok := err.(*url.Error); ok {
		return urlErr.Err
	}
	return nil
}
