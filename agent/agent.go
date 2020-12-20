package agent

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
)

var mA *MetaAgent

type MetaAgent struct {
	db *gorm.DB

	// Entity代理相关
	// todo 换成sync.Map
	pool     map[string]NewFunc
	listPool map[string]NewFunc
}

func NewMetaAgent(db *gorm.DB) *MetaAgent {
	ma := &MetaAgent{
		db: db,
		pool: map[string]NewFunc{},
		listPool: map[string]NewFunc{},
	}
	return ma
}

func (ma *MetaAgent) GetDB(ctx context.Context) *gorm.DB {
	// 获取db连接
	db, err := ma.getTxConnFromContext(ctx)
	if err != nil {
		db = ma.db
	}
	return db
}

func (ma *MetaAgent) RegisterGinHandler(router gin.IRouter) {
	if ma == nil {
		panic("ma can not be nil")
	}
	registerEntityHandler(router, ma)
}

type Schema interface {
	SchemaName() string
	NewFunc() interface{}
	NewListFunc() interface{}
	GetID() int64
	SetID(int64)
}

// NewListFunc 返回值是指针，参照Entity的实现
type NewFunc func() interface{}

func (ma *MetaAgent) RegisterSchema(schema Schema) {
	// 注册单个model池
	if ma.pool == nil {
		ma.pool = map[string]NewFunc{}
	}
	ma.pool[schema.SchemaName()] = schema.NewFunc

	// 注册model列表池
	if ma.listPool == nil {
		ma.listPool = map[string]NewFunc{}
	}
	ma.listPool[schema.SchemaName()] = schema.NewListFunc
}

// GetModel 返回一个该schema的对象指针
func (ma *MetaAgent) GetModelPtr(schemaName string) (interface{}, bool) {
	newFunc, exist := ma.pool[schemaName]
	if !exist {
		return nil, false
	}
	return newFunc(), true
}

// GetModelList 返回一个该schema的对象切片指针
func (ma *MetaAgent) GetModelListPtr(schemaName string) (interface{}, bool) {
	newFunc, exist := ma.listPool[schemaName]
	if !exist {
		return nil, false
	}
	return newFunc(), true
}
