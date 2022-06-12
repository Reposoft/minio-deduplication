package metadata_test

import (
	"testing"

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

}

func TestNext(t *testing.T) {

}
