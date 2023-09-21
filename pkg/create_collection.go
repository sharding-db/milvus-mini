package pkg

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/parameterutil.go"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/sharding-db/milvus-mini/pkg/allocator"
	pb "github.com/sharding-db/milvus-mini/pkg/etcdpb"
	"github.com/sharding-db/milvus-mini/pkg/metas"
	"github.com/sharding-db/milvus-mini/pkg/model"
	"go.uber.org/zap"
)

type CreateCollectionTask struct {
	idAllocator allocator.Interface
	meta        metas.MetaTable
	// dbLocks     DBLockers
	// walWriter   WALWriter

	req *milvuspb.CreateCollectionRequest
}

type DBLockers interface {
	// GetCollectionLocker returns a that locks the whole collection
	GetCollectionLocker(collectionName string) sync.Locker
}

type WALWriter interface {
	WriteRecord(record WALRecord) error
}

type WALRecord interface{}

func NewCreateCollectionTask(
	idAllocator allocator.Interface,
	meta metas.MetaTable,
	// dbLocks DBLockers,
	// walWriter WALWriter,
	request *milvuspb.CreateCollectionRequest) *CreateCollectionTask {

	return &CreateCollectionTask{
		idAllocator: idAllocator,
		meta:        meta,
		req:         request,
		// dbLocks:     dbLocks,
		// walWriter:   walWriter,
	}
}

// func (t CreateCollectionTask) CheckAndPersist(ctx context.Context) error {

// }

func (t CreateCollectionTask) Execute(ctx context.Context) error {
	request := t.req

	// TDOO:
	// collectionLocker := t.dbLocks.GetCollectionLocker(request.GetCollectionName())
	// collectionLocker.Lock()
	// err := t.CheckAndPersist(ctx)
	// collectionLocker.Unlock()

	db, err := t.meta.GetDatabaseByName(ctx, t.req.GetDbName())
	if err != nil {
		return merr.WrapErrServiceUnavailable(err.Error())
	}

	collId, err := t.idAllocator.AllocOne()
	if err != nil {
		return merr.WrapErrServiceUnavailable(err.Error())
	}

	schema, err := prepareSchema(t.req)
	if err != nil {
		return err
	}

	ts := uint64(time.Now().Unix())

	partitions, err := t.assignPartitions(request, schema, collId, ts)
	if err != nil {
		return err
	}
	collection := model.Collection{
		CollectionID: collId,
		DBID:         db.ID,
		Name:         request.GetCollectionName(),
		Description:  schema.GetDescription(),
		AutoID:       schema.AutoID,
		Fields:       model.UnmarshalFieldModels(schema.Fields),
		// milvus-mini don't have any channels
		// VirtualChannelNames:  vchanNames,
		// PhysicalChannelNames: chanNames,
		// StartPositions:       toKeyDataPairs(startPositions),

		ShardsNum:          request.ShardsNum,
		ConsistencyLevel:   request.ConsistencyLevel,
		CreateTime:         ts, // [TODO]
		State:              pb.CollectionState_CollectionCreating,
		Partitions:         partitions,
		Properties:         request.Properties,
		EnableDynamicField: schema.EnableDynamicField,
	}

	err = t.meta.AddCollection(ctx, &collection)
	return err
}

func (t CreateCollectionTask) assignPartitions(request *milvuspb.CreateCollectionRequest, schema *schemapb.CollectionSchema, collId int64, ts uint64) ([]*model.Partition, error) {
	partitionNames := make([]string, 0)
	const defaultPartitionName = "default"

	_, err := typeutil.GetPartitionKeyFieldSchema(schema)
	if err == nil {
		partitionNums := request.GetNumPartitions()
		// double check, default num of physical partitions should be greater than 0
		if partitionNums <= 0 {
			return nil, merr.WrapErrParameterInvalidMsg("the specified partitions should be greater than 0 if partition key is used")
		}

		for i := int64(0); i < partitionNums; i++ {
			partitionNames = append(partitionNames, fmt.Sprintf("%s_%d", defaultPartitionName, i))
		}
	} else {
		// compatible with old versions <= 2.2.8
		partitionNames = append(partitionNames, defaultPartitionName)
	}

	partitionIDsStart, _, err := t.idAllocator.Alloc(uint32(len(partitionNames)))
	if err != nil {
		return nil, err
	}

	ret := make([]*model.Partition, len(partitionNames))
	for i := 0; i < len(partitionNames); i++ {
		ret[i] = &model.Partition{
			PartitionID:               partitionIDsStart + int64(i),
			PartitionName:             partitionNames[i],
			PartitionCreatedTimestamp: ts,
			CollectionID:              collId,
			State:                     pb.PartitionState_PartitionCreated,
		}
	}
	log.Info("assign partitions when create collection",
		zap.String("collectionName", request.GetCollectionName()),
		zap.Strings("t.partitionNames", partitionNames))

	return ret, nil
}

func prepareSchema(request *milvuspb.CreateCollectionRequest) (*schemapb.CollectionSchema, error) {
	var schema schemapb.CollectionSchema
	if err := proto.Unmarshal(request.GetSchema(), &schema); err != nil {
		return nil, err
	}

	if request.GetCollectionName() != schema.GetName() {
		return nil, merr.WrapErrParameterInvalid(schema.GetName(), request.GetCollectionName(), "collection name matches schema name")
	}

	err := checkDefaultValue(&schema)
	if err != nil {
		return nil, err
	}
	appendDynamicField(&schema)
	assignFieldID(&schema)
	appendSysFields(&schema)
	return &schema, nil
}

func appendDynamicField(schema *schemapb.CollectionSchema) {
	if schema.EnableDynamicField {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			Name:        common.MetaFieldName,
			Description: "dynamic schema",
			DataType:    schemapb.DataType_JSON,
			IsDynamic:   true,
		})
	}
}

func assignFieldID(schema *schemapb.CollectionSchema) {
	for idx := range schema.GetFields() {
		schema.Fields[idx].FieldID = int64(idx + common.StartOfUserFieldID)
	}
}

func appendSysFields(schema *schemapb.CollectionSchema) {
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      int64(common.RowIDField),
		Name:         common.RowIDFieldName,
		IsPrimaryKey: false,
		Description:  "row id",
		DataType:     schemapb.DataType_Int64,
	})
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      int64(common.TimeStampField),
		Name:         common.TimeStampFieldName,
		IsPrimaryKey: false,
		Description:  "time stamp",
		DataType:     schemapb.DataType_Int64,
	})
}

func checkDefaultValue(schema *schemapb.CollectionSchema) error {
	for _, fieldSchema := range schema.Fields {
		if fieldSchema.GetDefaultValue() != nil {
			switch fieldSchema.GetDefaultValue().Data.(type) {
			case *schemapb.ValueField_BoolData:
				if fieldSchema.GetDataType() != schemapb.DataType_Bool {
					return merr.WrapErrParameterInvalid("DataType_Bool", "not match", "default value type mismatches field schema type")
				}
			case *schemapb.ValueField_IntData:
				if fieldSchema.GetDataType() != schemapb.DataType_Int32 &&
					fieldSchema.GetDataType() != schemapb.DataType_Int16 &&
					fieldSchema.GetDataType() != schemapb.DataType_Int8 {
					return merr.WrapErrParameterInvalid("DataType_Int", "not match", "default value type mismatches field schema type")
				}
				defaultValue := fieldSchema.GetDefaultValue().GetIntData()
				if fieldSchema.GetDataType() == schemapb.DataType_Int16 {
					if defaultValue > math.MaxInt16 || defaultValue < math.MinInt16 {
						return merr.WrapErrParameterInvalidRange(math.MinInt16, math.MaxInt16, defaultValue, "default value out of range")
					}
				}
				if fieldSchema.GetDataType() == schemapb.DataType_Int8 {
					if defaultValue > math.MaxInt8 || defaultValue < math.MinInt8 {
						return merr.WrapErrParameterInvalidRange(math.MinInt8, math.MaxInt8, defaultValue, "default value out of range")
					}
				}
			case *schemapb.ValueField_LongData:
				if fieldSchema.GetDataType() != schemapb.DataType_Int64 {
					return merr.WrapErrParameterInvalid("DataType_Int64", "not match", "default value type mismatches field schema type")
				}
			case *schemapb.ValueField_FloatData:
				if fieldSchema.GetDataType() != schemapb.DataType_Float {
					return merr.WrapErrParameterInvalid("DataType_Float", "not match", "default value type mismatches field schema type")
				}
			case *schemapb.ValueField_DoubleData:
				if fieldSchema.GetDataType() != schemapb.DataType_Double {
					return merr.WrapErrParameterInvalid("DataType_Double", "not match", "default value type mismatches field schema type")
				}
			case *schemapb.ValueField_StringData:
				if fieldSchema.GetDataType() != schemapb.DataType_VarChar {
					return merr.WrapErrParameterInvalid("DataType_VarChar", "not match", "default value type mismatches field schema type")
				}
				maxLength, err := parameterutil.GetMaxLength(fieldSchema)
				if err != nil {
					return err
				}
				defaultValueLength := len(fieldSchema.GetDefaultValue().GetStringData())
				if int64(defaultValueLength) > maxLength {
					msg := fmt.Sprintf("the length (%d) of string exceeds max length (%d)", defaultValueLength, maxLength)
					return merr.WrapErrParameterInvalid("valid length string", "string length exceeds max length", msg)
				}
			default:
				panic("default value unsupport data type")
			}
		}
	}

	return nil
}
