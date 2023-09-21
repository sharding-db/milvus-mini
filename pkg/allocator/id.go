package allocator

import (
	"time"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// UniqueID is alias of typeutil.UniqueID
type UniqueID = typeutil.UniqueID

// Allocator interface is an interface for alloc id.
// Alloc allocates the id of the count number.
// AllocOne allocates one id.
// See GlobalIDAllocator for implementation details
type Interface interface {
	Alloc(count uint32) (UniqueID, UniqueID, error)
	AllocOne() (UniqueID, error)
}

type LocalTsAllocator struct{}

func (l *LocalTsAllocator) AllocOne() (UniqueID, error) {
	return time.Now().UnixNano(), nil
}

func (l *LocalTsAllocator) Alloc(count uint32) (UniqueID, UniqueID, error) {
	var start = time.Now().UnixNano()
	time.Sleep(time.Duration(count) * time.Nanosecond)
	return UniqueID(start), UniqueID(start + int64(count)), nil
}
