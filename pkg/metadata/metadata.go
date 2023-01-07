package metadata

import (
	"mime"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
)

const (
	// separator should reflect an http headers convention
	separator = "; "
)

type MetadataNext struct {
	UserMetadata    map[string]string
	ReplaceMetadata bool
}

func encodePath(value string) string {
	return strings.ReplaceAll(value, ";", "%3B")
}

func AppendPath(list string, value string) string {
	encoded := encodePath(value)
	if list == "" {
		return encoded
	}

	split := strings.Split(list, separator)
	for _, path := range split {
		if path == value {
			return list
		}
	}

	return list + separator + encoded
}

func NewMetadataNext(uploaded, existing minio.ObjectInfo) *MetadataNext {
	uploadpath := uploaded.Key
	downloadName := filepath.Base(uploaded.Key)
	uploaddir := filepath.Dir(uploaded.Key)
	if uploaddir == "." {
		uploaddir = ""
	}
	if uploaddir != "" {
		uploaddir = uploaddir + "/"
	}

	meta := make(map[string]string)
	// if existing != nil {
	// 	for k, v := range existing.UserMetadata {
	// 		meta[k] = v
	// 	}
	// }
	for k, v := range uploaded.UserMetadata {
		meta[k] = v
	}

	meta["content-type"] = uploaded.ContentType
	meta["content-disposition"] = mime.FormatMediaType("attachment", map[string]string{"filename": downloadName})

	uploadpaths := AppendPath(existing.UserMetadata["Uploadpaths"], uploadpath)
	uploaddirs := AppendPath(existing.UserMetadata["Uploaddir"], uploaddir)

	meta["Uploadpaths"] = uploadpaths
	// kept legacy name that was singular
	if uploaddirs != "" {
		meta["Uploaddir"] = uploaddirs
	}

	return &MetadataNext{
		UserMetadata:    meta,
		ReplaceMetadata: true, // TODO make false if we did not change _anything_
	}
}
