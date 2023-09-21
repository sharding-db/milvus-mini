package metas

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/pkg/errors"
	"github.com/sharding-db/milvus-mini/pkg/model"
)

type Timestamp = uint64

type MetaTable interface {
	GetDatabaseByName(ctx context.Context, dbName string) (*model.Database, error)
	GetCollectionByName(ctx context.Context, dbName string, collectionName string) (*model.Collection, error)
	AddCollection(ctx context.Context, coll *model.Collection) error
}

// LocalDiskWithMemoryCacheMeta implements MetaTable by storing metadata in local disk with cache in memory
// when read, it will read from memory
// when write, it will write to disk & memory
type LocalDiskWithMemoryCacheMeta struct {
	lock                    sync.RWMutex
	dbIndexedByName         map[string]*model.Database
	collectionIndexedByName map[string]*model.Collection

	diskMeta *DiskMeta
}

func NewLocalDiskWithMemoryCacheMeta(ctx context.Context, rootPath string) (*LocalDiskWithMemoryCacheMeta, error) {
	diskMeta, err := NewDiskMeta(rootPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create disk meta")
	}
	ret := &LocalDiskWithMemoryCacheMeta{
		dbIndexedByName:         make(map[string]*model.Database),
		collectionIndexedByName: make(map[string]*model.Collection),
		diskMeta:                diskMeta,
	}
	err = ret.Init(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to init meta")
	}
	return ret, nil
}

// Init	load all meta from disk create default database if not exists
func (m *LocalDiskWithMemoryCacheMeta) Init(ctx context.Context) error {
	dbs, err := m.diskMeta.GetAllDatabeses(ctx)
	if err != nil {
		return err
	}
	for _, db := range dbs {
		m.dbIndexedByName[db.Name] = db
	}
	if _, found := m.dbIndexedByName[util.DefaultDBName]; !found {
		err = m.diskMeta.AddDatabase(ctx, model.NewDefaultDatabase())
		if err != nil {
			return err
		}
		m.dbIndexedByName[util.DefaultDBName] = model.NewDefaultDatabase()
	}
	colls, err := m.diskMeta.GetAllCollections(ctx)
	if err != nil {
		return err
	}
	for _, coll := range colls {
		m.collectionIndexedByName[coll.Name] = coll
	}
	return nil
}

func (m *LocalDiskWithMemoryCacheMeta) GetDatabaseByName(ctx context.Context, dbName string) (*model.Database, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	ret, found := m.dbIndexedByName[dbName]
	if !found {
		return nil, merr.WrapErrDatabaseNotFound(dbName)
	}
	return ret, nil
}

func (m *LocalDiskWithMemoryCacheMeta) GetCollectionByName(ctx context.Context, dbName string, collectionName string) (*model.Collection, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	ret, found := m.collectionIndexedByName[collectionName]
	if !found {
		return nil, merr.WrapErrCollectionNotFound(collectionName)
	}
	return ret, nil
}

func (m *LocalDiskWithMemoryCacheMeta) AddCollection(ctx context.Context, newColl *model.Collection) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	collection, found := m.collectionIndexedByName[newColl.Name]
	if found {
		if collection.Equal(*newColl) {
			return nil
		}
		return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("create duplicate collection with different parameters, collection: %s", newColl.Name))
	}
	err := m.diskMeta.AddCollection(ctx, newColl)
	if err != nil {
		return err
	}
	m.collectionIndexedByName[newColl.Name] = newColl
	return nil
}

type DiskMeta struct {
	rootPath string
}

func NewDiskMeta(rootPath string) (*DiskMeta, error) {
	ret := &DiskMeta{
		rootPath: rootPath,
	}
	err := ret.Init()
	return ret, err
}

func (m *DiskMeta) Init() error {
	err := os.MkdirAll(m.rootPath, 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(fmt.Sprintf("%s/%s", m.rootPath, DBInfoMetaPrefix), 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(fmt.Sprintf("%s/%s", m.rootPath, CollectionInfoMetaPrefix), 0755)
	if err != nil {
		return err
	}
	return nil
}

func (m *DiskMeta) GetAllDatabeses(ctx context.Context) ([]*model.Database, error) {
	files, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", m.rootPath, DBInfoMetaPrefix))
	if err != nil {
		return nil, err
	}
	ret := make([]*model.Database, 0, len(files))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		obj := new(model.Database)
		err = m.GetObject(ctx, fmt.Sprintf("%s/%s", DBInfoMetaPrefix, file.Name()), obj)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get database[%s]", file.Name())
		}
	}
	return ret, nil
}

func (m *DiskMeta) GetAllCollections(ctx context.Context) ([]*model.Collection, error) {
	files, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", m.rootPath, CollectionInfoMetaPrefix))
	if err != nil {
		return nil, errors.Wrap(err, "failed to list collections in disk")
	}
	ret := make([]*model.Collection, 0, len(files))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		obj := new(model.Collection)
		err = m.GetObject(ctx, fmt.Sprintf("%s/%s", CollectionInfoMetaPrefix, file.Name()), obj)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get collection[%s]", file.Name())
		}
	}
	return ret, nil
}

func (m *DiskMeta) AddDatabase(ctx context.Context, newDB *model.Database) error {
	key := BuildDatabaseKey(newDB.ID)
	return m.AddObject(ctx, key, newDB)
}

func (m *DiskMeta) AddCollection(ctx context.Context, newColl *model.Collection) error {
	key := BuildCollectionKeyWithDBID(newColl.DBID, newColl.CollectionID)
	err := m.AddObject(ctx, key, newColl)
	return errors.Wrapf(err, "failed to add key[%s]", key)
}

func (m *DiskMeta) GetObject(ctx context.Context, key string, obj any) error {
	fileName := fmt.Sprintf("%s/%s", m.rootPath, key)
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, obj)
	return err
}

func (m *DiskMeta) AddObject(ctx context.Context, key string, obj any) error {
	fileName := fmt.Sprintf("%s/%s", m.rootPath, key)
	dir := filepath.Dir(fileName)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	value, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %s", err.Error())
	}
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(value)
	if err != nil {
		return err
	}
	err = file.Sync()
	return err
}
