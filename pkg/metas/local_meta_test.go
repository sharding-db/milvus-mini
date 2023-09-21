package metas

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalDiskWithMemoryCacheMeta(t *testing.T) {
	rootPath, err := ioutil.TempDir("/tmp/", "milvus-*")
	assert.NoError(t, err)
	_, err = NewLocalDiskWithMemoryCacheMeta(context.Background(), rootPath)
	assert.NoError(t, err)
}
