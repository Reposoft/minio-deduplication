package metadata_test

import (
	"testing"

	"github.com/minio/minio-go/v7"
	"repos.se/minio-deduplication/v2/pkg/metadata"
)

func TestAppend(t *testing.T) {

	start := metadata.AppendPath("", "my/path.png")
	if start != "my/path.png" {
		t.Errorf("Unexpected %s", start)
	}

	next := metadata.AppendPath(start, "/absolute/path.png")
	if next != "my/path.png; /absolute/path.png" {
		t.Errorf("Unexpected %s", next)
	}

	escaped := metadata.AppendPath(next, "; strange;PATH.jpeg")
	if escaped != "my/path.png; /absolute/path.png; %3B strange%3BPATH.jpeg" {
		t.Errorf("Unexpected %s", escaped)
	}

	duplicate := metadata.AppendPath(escaped, "/absolute/path.png")
	if duplicate != "my/path.png; /absolute/path.png; %3B strange%3BPATH.jpeg" {
		t.Errorf("Unexpected %s", duplicate)
	}

	dir := metadata.AppendPath("", "my dir/")
	if dir != "my dir/" {
		t.Errorf("Unexpected %s", dir)
	}

	dir2 := metadata.AppendPath(dir, "my% other%/dir/")
	if dir2 != "my dir/; my% other%/dir/" {
		t.Errorf("Unexpected %s", dir2)
	}

}

func TestNext(t *testing.T) {

	var existing minio.ObjectInfo
	upload1 := &minio.ObjectInfo{
		Key:          "My File.txt",
		ContentType:  "text/plain",
		UserMetadata: make(minio.StringMap),
	}
	next := metadata.NewMetadataNext(*upload1, existing)

	if !next.ReplaceMetadata {
		t.Error("ReplaceMetadata should be true when there's changes")
	}
	if next.UserMetadata["content-type"] != "text/plain" {
		t.Errorf("Unexpected content-type: %s", next.UserMetadata["content-type"])
	}
	if next.UserMetadata["content-disposition"] != "attachment; filename=\"My File.txt\"" {
		t.Errorf("Unexpected content-disposition: %s", next.UserMetadata["content-disposition"])
	}
	if next.UserMetadata["Uploadpaths"] != "My File.txt" {
		t.Errorf("Unexpected Uploadpaths: %s", next.UserMetadata["Uploadpaths"])
	}
	dir1, hasDir1 := next.UserMetadata["Uploaddir"]
	if hasDir1 {
		t.Errorf("Uploaddir should not be set for files without dir path element, got: %s", dir1)
	}

	upload2 := &minio.ObjectInfo{
		Key:          "some ; dir/sub/My File new.txt",
		ContentType:  "text/plain",
		UserMetadata: make(minio.StringMap),
	}
	next2 := metadata.NewMetadataNext(*upload2, minio.ObjectInfo{
		Key:         "ab/cd/abcde.txt",
		ContentType: "text/anything",
		UserMetadata: minio.StringMap{
			"Uploadpaths": next.UserMetadata["Uploadpaths"],
			"Uploaddir":   next.UserMetadata["Uploaddir"],
		},
	})

	if !next2.ReplaceMetadata {
		t.Error("ReplaceMetadata should be true when there's changes")
	}
	if next2.UserMetadata["content-type"] != "text/plain" {
		t.Errorf("Unexpected content-type: %s", next.UserMetadata["content-type"])
	}
	if next2.UserMetadata["content-disposition"] != "attachment; filename=\"My File new.txt\"" {
		t.Errorf("Unexpected content-disposition: %s", next.UserMetadata["content-disposition"])
	}
	if next2.UserMetadata["Uploadpaths"] != "My File.txt; some %3B dir/sub/My File new.txt" {
		t.Errorf("Unexpected Uploadpaths: %s", next.UserMetadata["Uploadpaths"])
	}
	dir2 := next2.UserMetadata["Uploaddir"]
	if dir2 != "some %3B dir/sub/" {
		t.Errorf("Unexpected Uploaddir: %s", dir2)
	}

}
