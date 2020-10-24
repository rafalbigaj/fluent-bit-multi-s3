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
	"C"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

type PluginContext struct {
	ClientSet              *kubernetes.Clientset
	S3SecretNameAnnotation string
	PipelineRunLabel       string
	PipelineTaskLabel      string
}

func NewPluginContext(plugin unsafe.Pointer, client *kubernetes.Clientset) *PluginContext {
	ctx := &PluginContext{ClientSet: client}
	ctx.S3SecretNameAnnotation = getPluginConfig(plugin, "S3_Secret_Name_Annotation", "orchestration/artifact_secret")
	ctx.PipelineRunLabel = getPluginConfig(plugin, "Pipeline_Run_Label", "tekton.dev/pipelineRun")
	ctx.PipelineTaskLabel = getPluginConfig(plugin, "Pipeline_Task_Label", "tekton.dev/pipelineTask")
	return ctx
}

func getPluginConfig(plugin unsafe.Pointer, configKey string, defValue string) string {
	config := output.FLBPluginConfigKey(plugin, configKey)
	if config == "" {
		return defValue
	}
	return config
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "multi-s3", "Distribute logs to the multiple s3 instances.")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	klog.InitFlags(nil)

	kubeConfig := os.Getenv("KUBECONFIG") // only required if out-of-cluster
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		klog.Fatalln(err.Error())
		return output.FLB_ERROR
	}

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalln(err.Error())
		return output.FLB_ERROR
	}

	ctx := NewPluginContext(plugin, clientSet)

	klog.Infof("[multi-s3] S3SecretNameAnnotation: %s", ctx.S3SecretNameAnnotation)
	klog.Infof("[multi-s3] PipelineRunLabel: %s", ctx.PipelineRunLabel)
	klog.Infof("[multi-s3] PipelineTaskLabel: %s", ctx.PipelineTaskLabel)

	output.FLBPluginSetContext(plugin, ctx)

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(_ unsafe.Pointer, _ C.int, _ *C.char) int {
	klog.Warning("[multi-s3] Flush called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, cTag *C.char) int {
	// Type assert context back into the original type for the Go variable
	tag := C.GoString(cTag)
	// Get plugin context (*PluginContext) initialized in `FLBPluginInit`
	pluginCtx := output.FLBPluginGetContext(ctx).(*PluginContext)
	kubeClient := pluginCtx.ClientSet

	// input-cri-o produces tags in form: "kube.<namespace_name>.<pod_name>"
	s := strings.Split(tag, ".")
	namespace := s[1]
	podName := s[2]
	klog.Infof("[multi-s3] Flush called for namespace: %q, pod: %q", namespace, podName)

	pod, err := kubeClient.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Errorf("[multi-s3] Pod %q not found (%s). Skipping logs...", podName, err)
			return output.FLB_ERROR // no reason to retry
		} else {
			klog.Warningf("[multi-s3] Unable to retrieve Pod %q metadata: %s.", podName, err)
			return output.FLB_RETRY
		}
	}

	secretName := pod.Annotations[pluginCtx.S3SecretNameAnnotation]
	if secretName == "" {
		klog.Errorf("[multi-s3] The secret name for artifact repository is not set.")
		return output.FLB_ERROR // no way to recover
	}
	klog.Infof("[multi-s3] Secret: %q", secretName)

	secret, err := kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Errorf("[multi-s3] Secret %q not found (%s). Skipping logs...", secretName, err)
			return output.FLB_ERROR // no reason to retry
		} else {
			klog.Infof("[multi-s3] Warning! Unable to retrieve secret %q: %s.", secretName, err)
			return output.FLB_RETRY
		}
	}

	s3ctx, err := LoadS3BucketContextFromSecret(secret)
	if err != nil {
		klog.Errorf("[multi-s3] Invalid s3 secret %q. %s", secretName, err)
		return output.FLB_ERROR // no reason to retry
	}

	pipelineRun := pod.Labels[pluginCtx.PipelineRunLabel]
	pipelineTask := pod.Labels[pluginCtx.PipelineTaskLabel]

	var buf bytes.Buffer
	b := C.GoBytes(data, C.int(length))
	result := ArchiveLog(b, &buf)
	if result != output.FLB_OK {
		return result
	}

	key := fmt.Sprintf("artifacts/%s/%s/step-main.tgz", pipelineRun, pipelineTask)
	klog.Infof("[multi-s3] Uploading file: %s (%d bytes)...", key, buf.Len())
	err = PutLogObject(s3ctx, key, &buf, int64(buf.Len()))
	if err != nil {
		klog.Errorf("[multi-s3] Error in uploading file %q to s3: %s.", key, err)
		return output.FLB_RETRY
	}
	klog.Infof("[multi-s3] OK")

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}

func createMinioClient(endpoint string, endpointSchema string, accessKey string, secretKey string) (*minio.Client, error) {
	secure := strings.HasPrefix(endpointSchema, "https")
	cred := credentials.NewStaticV4(accessKey, secretKey, "")

	return minio.NewWithCredentials(endpoint, cred, secure, "")
}

type RunLogEntry struct {
	Log       string    `json:"log"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

// ArchiveLog decodes records (msgpack) and rewrites them to JSON and store in `dst` as a compressed (gzip)
// Fluent-bit records that come from "cri-o" parser contain a timestamp (as FLBTime or int), "stream" (ignored) and "log".
func ArchiveLog(data []byte, dst io.Writer) int {
	gw := gzip.NewWriter(dst)
	ptr := C.CBytes(data)
	dec := output.NewDecoder(ptr, len(data))

	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		timestamp := getRecordTimestamp(ts)

		if logVal := record["log"]; logVal != nil {
			if log, ok := logVal.([]byte); ok {
				entry := RunLogEntry{Timestamp: timestamp, Log: string(log)}
				eb, err := json.Marshal(entry)
				if err == nil {
					err = writeBytesLn(gw, eb)
				}
				if err != nil {
					klog.Error("[multi-s3] Error in writing to tarball:", err)
					return output.FLB_ERROR
				}
			}
		}
	}

	err := gw.Close()
	if err != nil {
		klog.Error("[multi-s3] Error in closing gzip archive:", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

func getRecordTimestamp(ts interface{}) (timestamp time.Time) {
	switch t := ts.(type) {
	case output.FLBTime:
		timestamp = t.Time
	case int64:
		timestamp = time.Unix(t, 0)
	case uint64:
		timestamp = time.Unix(int64(t), 0)
	default:
		klog.Warningln("time provided invalid, defaulting to now.")
		timestamp = time.Now()
	}
	return
}

func writeBytesLn(dst io.Writer, bytes []byte) (err error) {
	_, err = dst.Write(bytes)
	if err != nil {
		return
	}
	_, err = dst.Write([]byte{'\n'})
	return
}

type S3BucketContext struct {
	Endpoint       string
	EndpointSchema string
	AccessKey      string
	SecretKey      string
	Bucket         string
}

// PutLogObject creates a new object in the bucket with the given key and content from reader.
// If the object with the key already exists its content is merged with the new one before upload.
func PutLogObject(ctx *S3BucketContext, key string, reader io.Reader, objectSize int64) (err error) {
	minioClient, err := createMinioClient(ctx.Endpoint, ctx.EndpointSchema, ctx.AccessKey, ctx.SecretKey)
	if err != nil {
		return
	}

	mergedReader := reader
	mergedSize := objectSize
	object, err := minioClient.GetObject(ctx.Bucket, key, minio.GetObjectOptions{})
	if err == nil {
		if objectInfo, getErr := object.Stat(); getErr == nil {
			klog.Infof("[multi-s3] Found existing s3 object: %s (%d bytes), merging...", key, objectInfo.Size)
			var buf bytes.Buffer
			err = mergeObjects(object, reader, &buf)
			if err != nil {
				return
			}
			mergedReader = &buf
			mergedSize = int64(buf.Len())
			klog.Infof("[multi-s3] Merged s3 object: %s (%d bytes).", key, mergedSize)
		} else if res, ok := getErr.(minio.ErrorResponse); !ok || res.StatusCode != http.StatusNotFound {
			err = getErr
			return
		}
	} else {
		return
	}

	_, err = minioClient.PutObject(ctx.Bucket, key, mergedReader, mergedSize, minio.PutObjectOptions{})

	return
}

type S3Credentials struct {
	ApiKey    string `json:"api_key",omitempty`
	ServiceId string `json:"service_id",omitempty`
	AccessKey string `json:"access_key_id",omitempty`
	SecretKey string `json:"secret_access_key",omitempty`
}

type S3CredentialsPerRole struct {
	Admin  S3Credentials `json:"admin"`
	Editor S3Credentials `json:"editor"`
	Viewer S3Credentials `json:"viewer"`
}

type S3Properties struct {
	BucketName  string               `json:"bucket_name"`
	EndpointUrl string               `json:"endpoint_url"`
	Credentials S3CredentialsPerRole `json:"credentials"`
}

type S3Config struct {
	Type       string       `json:"type,omitempty"`
	Properties S3Properties `json:"properties,omitempty"`
}

func LoadS3Config(data []byte) (*S3Config, error) {
	var credentials S3Config
	if err := json.Unmarshal(data, &credentials); err != nil {
		return nil, err
	}
	return &credentials, nil
}

func LoadS3BucketContextFromSecret(secret *v1.Secret) (*S3BucketContext, error) {
	config, ok := secret.Data["config"]
	if !ok {
		return nil, fmt.Errorf("no config key")
	}
	s3Config, err := LoadS3Config(config)
	if err != nil {
		return nil, fmt.Errorf("unable to parse s3 credentials: %s", err)
	}

	endpointUrl, err := url.Parse(s3Config.Properties.EndpointUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid s3 endpoint URL: %s, %s", s3Config.Properties.EndpointUrl, err)
	}

	s3ctx := S3BucketContext{
		Endpoint:       endpointUrl.Host,
		EndpointSchema: endpointUrl.Scheme,
		AccessKey:      s3Config.Properties.Credentials.Editor.AccessKey,
		SecretKey:      s3Config.Properties.Credentials.Editor.SecretKey,
		Bucket:         s3Config.Properties.BucketName,
	}

	return &s3ctx, nil
}

func mergeObjects(a, b io.Reader, dst io.Writer) (err error) {
	da, err := gzip.NewReader(a)
	if err != nil {
		return
	}
	db, err := gzip.NewReader(b)
	if err != nil {
		return
	}
	cd := gzip.NewWriter(dst)

	if _, err = io.Copy(cd, da); err != nil {
		return
	}
	if _, err = io.Copy(cd, db); err != nil {
		return
	}
	if err = cd.Close(); err != nil {
		return
	}

	return
}
