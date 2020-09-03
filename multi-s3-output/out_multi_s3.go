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
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

type PluginContext struct {
	ClientSet                        *kubernetes.Clientset
	ArtifactEndpointAnnotation       string
	ArtifactEndpointSchemeAnnotation string
	ArtifactBucketAnnotation         string
	ArtifactSecretAnnotation         string
	PipelineRunLabel                 string
	PipelineTaskLabel                string
	PipelineTaskRunLabel             string
}

func NewPluginContext(plugin unsafe.Pointer, client *kubernetes.Clientset) *PluginContext {
	ctx := &PluginContext{ClientSet: client}
	ctx.ArtifactEndpointAnnotation = getPluginConfig(plugin, "Artifact_Endpoint_Annotation", "tekton.dev/artifact_endpoint")
	ctx.ArtifactEndpointSchemeAnnotation = getPluginConfig(plugin, "Artifact_Endpoint_Scheme_Annotation", "tekton.dev/artifact_endpoint_scheme")
	ctx.ArtifactBucketAnnotation = getPluginConfig(plugin, "Artifact_Bucket_Annotation", "tekton.dev/artifact_bucket")
	ctx.ArtifactSecretAnnotation = getPluginConfig(plugin, "Artifact_Secret_Annotation", "tekton.dev/artifact_secret")
	ctx.PipelineRunLabel = getPluginConfig(plugin, "Pipeline_Run_Label", "tekton.dev/pipelineRun")
	ctx.PipelineTaskLabel = getPluginConfig(plugin, "Pipeline_Task_Label", "tekton.dev/pipelineTask")
	ctx.PipelineTaskRunLabel = getPluginConfig(plugin, "Pipeline_Task_Run_Label", "tekton.dev/taskRun")
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

	var err error
	var config *rest.Config
	kubeConfig := os.Getenv("KUBECONFIG") // only required if out-of-cluster
	config, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
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

	output.FLBPluginSetContext(plugin, ctx)

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
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

	secretName := pod.Annotations[pluginCtx.ArtifactSecretAnnotation]
	if secretName == "" {
		klog.Warningf("[multi-s3] The secret name for artifact repository is not set. Using default...")
		secretName = "mlpipeline-minio-artifact"
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

	pipelineRun := pod.Labels[pluginCtx.PipelineRunLabel]
	pipelineTask := pod.Labels[pluginCtx.PipelineTaskLabel]
	pipelineTaskRunId := pod.Labels[pluginCtx.PipelineTaskRunLabel]

	accessKey, ok := secret.Data["accesskey"]
	if !ok {
		klog.Errorf("[multi-s3] Invalid s3 secret %q. No access key.", secretName)
		return output.FLB_ERROR // no reason to retry
	}
	secretKey, ok := secret.Data["secretkey"]
	if !ok {
		klog.Errorf("[multi-s3] Invalid s3 secret %q. No secret key.", secretName)
		return output.FLB_ERROR // no reason to retry
	}

	s3ctx := S3BucketContext{
		Endpoint:       pod.Annotations[pluginCtx.ArtifactEndpointAnnotation],
		EndpointSchema: pod.Annotations[pluginCtx.ArtifactEndpointSchemeAnnotation],
		AccessKey:      string(accessKey),
		SecretKey:      string(secretKey),
		Bucket:         pod.Annotations[pluginCtx.ArtifactBucketAnnotation],
	}

	var buf bytes.Buffer
	b := C.GoBytes(data, C.int(length))
	result := ArchiveLog(b, &buf)
	if result != output.FLB_OK {
		return result
	}

	key := fmt.Sprintf("artifacts/%s/%s/%s--step-main_log.gz", pipelineRun, pipelineTask, pipelineTaskRunId)
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
	secure := endpointSchema == "https://"
	cred := credentials.NewStaticV4(accessKey, secretKey, "")

	return minio.NewWithCredentials(endpoint, cred, secure, "")
}

type RunLogEntry struct {
	Log       string    `json:"log"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

func ArchiveLog(data []byte, dst io.Writer) int {
	gw := gzip.NewWriter(dst)
	ptr := C.CBytes(data)
	dec := output.NewDecoder(ptr, len(data))

	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		var timestamp time.Time
		switch t := ts.(type) {
		case output.FLBTime:
			timestamp = ts.(output.FLBTime).Time
		case int64:
			timestamp = time.Unix(t, 0)
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			fmt.Println("time provided invalid, defaulting to now.")
			timestamp = time.Now()
		}

		var logKey interface{} = "log"
		logVal := record[logKey]
		if logVal != nil && !reflect.ValueOf(logVal).IsNil() {
			log := string(logVal.([]byte))
			entry := RunLogEntry{Timestamp: timestamp, Log: log}
			bytes, err := json.Marshal(entry)
			if err == nil {
				err = writeBytesLn(gw, bytes)
			}
			if err != nil {
				klog.Error("[multi-s3] Error in writing to tarball:", err)
				return output.FLB_ERROR
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

func PutLogObject(ctx S3BucketContext, key string, reader io.Reader, objectSize int64) (err error) {
	minioClient, err := createMinioClient(ctx.Endpoint, ctx.EndpointSchema, ctx.AccessKey, ctx.SecretKey)
	if err != nil {
		return
	}

	mergedReader := reader
	mergedSize := objectSize
	if object, getErr := minioClient.GetObject(ctx.Bucket, key, minio.GetObjectOptions{}); getErr == nil {
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
		}
	}

	_, err = minioClient.PutObject(ctx.Bucket, key, mergedReader, mergedSize, minio.PutObjectOptions{})

	return
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
