package main

import (
	"context"
	"log"
	"net"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/sharding-db/milvus-mini/pkg"
	"github.com/sharding-db/milvus-mini/pkg/allocator"
	"github.com/sharding-db/milvus-mini/pkg/metas"

	"google.golang.org/grpc"
)

func main() {
	// create listiner
	lis, err := net.Listen("tcp", ":19530")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// create grpc server
	s := grpc.NewServer()
	ctx := context.Background()
	log.Println("init meta table")
	rootPath := "./tmp/milvus-mini"
	metatable, err := metas.NewLocalDiskWithMemoryCacheMeta(ctx, rootPath)
	if err != nil {
		log.Fatalf("failed to create meta table: %v", err)
	}
	miniMilvus := pkg.NewMilvusMini(new(allocator.LocalTsAllocator), metatable)
	milvuspb.RegisterMilvusServiceServer(s, miniMilvus)

	log.Println("start server on 19530")
	// and start...
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
