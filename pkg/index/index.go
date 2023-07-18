package index

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"repos.se/minio-deduplication/v2/pkg/metadata"
)

type IndexEntry struct {
	IndexFormatVersion int8 `json:"v"`
	// Upload is the original uploadPath
	Upload string `json:"upload"`
	// Key is the blob key for read access
	Key string `json:"key"`
	// Replaced is true if the target existed (i.e. identical body), false if new
	Replaced bool `json:"replaced"`
	// Metareplaced is true if meta was updated, false if unchanged or new
	Metareplaced bool `json:"metareplaced"`
	// Etag is the upload etag
	Etag string `json:"etag"`
	// Meta is the metadata written
	Meta map[string]string `json:"meta"`
}

type Index struct {
	entries []IndexEntry
}

func New() *Index {
	return &Index{}
}

func (i *Index) Size() int {
	return len(i.entries)
}

func (i *Index) Append(entry IndexEntry) {
	i.entries = append(i.entries, entry)
}

func (i *Index) AppendTransfer(uploadKey string, dstInfo minio.UploadInfo, replaced bool, meta *metadata.MetadataNext) {
	// note that dstInfo.Size is zero because we did a copy
	i.Append(IndexEntry{
		IndexFormatVersion: 1,
		Upload:             uploadKey,
		Key:                dstInfo.Key,
		Replaced:           replaced,
		Metareplaced:       replaced && meta.ReplaceMetadata,
		Etag:               dstInfo.ETag,
		Meta:               meta.UserMetadata,
	})
}

// Serialize returns what to write and the content-type, or error
func (i *Index) Serialize(contentType string) (io.Reader, int64, error) {
	if contentType != "application/jsonlines" {
		return nil, 0, fmt.Errorf("unsupported content-type %s", contentType)
	}

	// we could probably be clever and serialize on reader Read, but let's do that later
	separator := []byte{'\n'}
	buf := bytes.NewBuffer([]byte{})
	for _, entry := range i.entries {
		jsonline, err := json.Marshal(entry)
		if err != nil {
			return nil, 0, err
		}
		_, err = buf.Write(jsonline)
		if err != nil {
			return nil, 0, err
		}
		_, err = buf.Write(separator)
		if err != nil {
			return nil, 0, err
		}
	}

	return buf, int64(buf.Len()), nil
}
