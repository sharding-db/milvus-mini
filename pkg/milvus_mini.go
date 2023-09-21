package pkg

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/federpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/pkg/errors"
	"github.com/sharding-db/milvus-mini/pkg/allocator"
	"github.com/sharding-db/milvus-mini/pkg/metas"
)

type MilvusMini struct {
	idAllocator allocator.Interface
	meta        metas.MetaTable
}

func NewMilvusMini(idAllocator allocator.Interface, meta metas.MetaTable) *MilvusMini {
	return &MilvusMini{
		idAllocator: idAllocator,
		meta:        meta,
	}
}

func (m *MilvusMini) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	if request.DbName == "" {
		request.DbName = util.DefaultDBName
	}
	err := NewCreateCollectionTask(m.idAllocator, m.meta, request).Execute(ctx)
	return merr.Status(err), nil
}

func (m *MilvusMini) DropCollection(context.Context, *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	_, err := m.meta.GetCollectionByName(ctx, req.DbName, req.CollectionName)
	if err != nil {
		return &milvuspb.BoolResponse{Value: false, Status: merr.Status(err)}, nil
	}
	return &milvuspb.BoolResponse{Value: true}, nil
}
func (m *MilvusMini) LoadCollection(context.Context, *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ReleaseCollection(context.Context, *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	_, err := m.meta.GetCollectionByName(ctx, req.DbName, req.CollectionName)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return &milvuspb.DescribeCollectionResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "can't find collection"}}, nil
		}
		return &milvuspb.DescribeCollectionResponse{Status: merr.Status(err)}, nil
	}
	return &milvuspb.DescribeCollectionResponse{}, nil
}
func (m *MilvusMini) GetCollectionStatistics(context.Context, *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ShowCollections(context.Context, *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) AlterCollection(context.Context, *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) CreatePartition(context.Context, *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DropPartition(context.Context, *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) HasPartition(context.Context, *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) LoadPartitions(context.Context, *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ReleasePartitions(context.Context, *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetPartitionStatistics(context.Context, *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ShowPartitions(context.Context, *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetLoadingProgress(context.Context, *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetLoadState(context.Context, *milvuspb.GetLoadStateRequest) (*milvuspb.GetLoadStateResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) CreateAlias(context.Context, *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DropAlias(context.Context, *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) AlterAlias(context.Context, *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DescribeAlias(context.Context, *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ListAliases(context.Context, *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) CreateIndex(context.Context, *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DescribeIndex(context.Context, *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetIndexStatistics(context.Context, *milvuspb.GetIndexStatisticsRequest) (*milvuspb.GetIndexStatisticsResponse, error) {
	return nil, errors.Errorf("TODO")
}

// Deprecated: use DescribeIndex instead
func (m *MilvusMini) GetIndexState(context.Context, *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	return nil, errors.Errorf("TODO")
}

// Deprecated: use DescribeIndex instead
func (m *MilvusMini) GetIndexBuildProgress(context.Context, *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DropIndex(context.Context, *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) Insert(context.Context, *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) Delete(context.Context, *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) Upsert(context.Context, *milvuspb.UpsertRequest) (*milvuspb.MutationResult, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) Search(context.Context, *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) Flush(context.Context, *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) Query(context.Context, *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) CalcDistance(context.Context, *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) FlushAll(context.Context, *milvuspb.FlushAllRequest) (*milvuspb.FlushAllResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetFlushState(context.Context, *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetFlushAllState(context.Context, *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetPersistentSegmentInfo(context.Context, *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetQuerySegmentInfo(context.Context, *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetReplicas(context.Context, *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) Dummy(context.Context, *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	return nil, errors.Errorf("TODO")
}

// TODO: remove
func (m *MilvusMini) RegisterLink(context.Context, *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	return nil, errors.Errorf("TODO")
}

// https://wiki.lfaidata.foundation/display/MIL/MEP+8+--+Add+metrics+for+proxy
func (m *MilvusMini) GetMetrics(context.Context, *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetComponentStates(context.Context, *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) LoadBalance(context.Context, *milvuspb.LoadBalanceRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetCompactionState(context.Context, *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ManualCompaction(context.Context, *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetCompactionStateWithPlans(context.Context, *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return nil, errors.Errorf("TODO")
}

// https://wiki.lfaidata.foundation/display/MIL/MEP+24+--+Support+bulk+load
func (m *MilvusMini) Import(context.Context, *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetImportState(context.Context, *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ListImportTasks(context.Context, *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	return nil, errors.Errorf("TODO")
}

// https://wiki.lfaidata.foundation/display/MIL/MEP+27+--+Support+Basic+Authentication
func (m *MilvusMini) CreateCredential(context.Context, *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) UpdateCredential(context.Context, *milvuspb.UpdateCredentialRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DeleteCredential(context.Context, *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ListCredUsers(context.Context, *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return nil, errors.Errorf("TODO")
}

// https://wiki.lfaidata.foundation/display/MIL/MEP+29+--+Support+Role-Based+Access+Control
func (m *MilvusMini) CreateRole(context.Context, *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DropRole(context.Context, *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) OperateUserRole(context.Context, *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) SelectRole(context.Context, *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) SelectUser(context.Context, *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) OperatePrivilege(context.Context, *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) SelectGrant(context.Context, *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) GetVersion(context.Context, *milvuspb.GetVersionRequest) (*milvuspb.GetVersionResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) CheckHealth(context.Context, *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) CreateResourceGroup(context.Context, *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DropResourceGroup(context.Context, *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) TransferNode(context.Context, *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) TransferReplica(context.Context, *milvuspb.TransferReplicaRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ListResourceGroups(context.Context, *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DescribeResourceGroup(context.Context, *milvuspb.DescribeResourceGroupRequest) (*milvuspb.DescribeResourceGroupResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) RenameCollection(context.Context, *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ListIndexedSegment(context.Context, *federpb.ListIndexedSegmentRequest) (*federpb.ListIndexedSegmentResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DescribeSegmentIndexData(context.Context, *federpb.DescribeSegmentIndexDataRequest) (*federpb.DescribeSegmentIndexDataResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) Connect(context.Context, *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) AllocTimestamp(context.Context, *milvuspb.AllocTimestampRequest) (*milvuspb.AllocTimestampResponse, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) CreateDatabase(context.Context, *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) DropDatabase(context.Context, *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return nil, errors.Errorf("TODO")
}
func (m *MilvusMini) ListDatabases(context.Context, *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return nil, errors.Errorf("TODO")
}
