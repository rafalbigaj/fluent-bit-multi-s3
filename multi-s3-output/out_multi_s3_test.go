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
	"github.com/stretchr/testify/assert"
	"github.com/ugorji/go/codec"
	"io/ioutil"
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
	unixTs := ts.Unix()
	assert.Nil(t, err)
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
