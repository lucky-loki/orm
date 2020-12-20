package agent

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
	"github.com/lucky-loki/orm/agent/utils"
)

func Init(db *gorm.DB) {
	db.LogMode(true)
	if mA == nil {
		mA = &MetaAgent{}
	}
	mA.db = db
	mA.RegisterSchema(new(EntityRelation))
	err := db.AutoMigrate(new(EntityRelation)).Error
	if err != nil {
		panic(err)
	}
}

// InitGinHandler 必须在Init执行后才能执行
func RegisterGinHandler(router gin.IRouter) {
	if mA == nil {
		panic("mA not init")
	}
	mA.RegisterGinHandler(router)
}

func RegisterSchema(schema Schema) {
	if mA == nil {
		panic("mA not init")
	}
	mA.RegisterSchema(schema)
}

func GetDB(ctx context.Context) *gorm.DB {
	if mA == nil {
		panic("mA not init")
	}
	return mA.GetDB(ctx)
}

func GetModelPtr(schemaName string) (interface{}, bool) {
	if mA == nil {
		panic("mA not init")
	}
	return mA.GetModelPtr(schemaName)
}

func GetModelListPtr(schemaName string) (interface{}, bool) {
	if mA == nil {
		panic("mA not init")
	}
	return mA.GetModelListPtr(schemaName)
}

func CreateEntity(ctx context.Context, mPtr interface{}) error {
	if mA == nil {
		panic("mA not init")
	}
	return mA.CreateEntity(ctx, mPtr)
}

// 全量更新，如果某字段为空就意味着更新为空值
func UpdateEntityByID(ctx context.Context, mPtr interface{}) error {
	if mA == nil {
		panic("mA not init")
	}
	return mA.UpdateEntityByID(ctx, mPtr)
}

func DeleteEntityByID(ctx context.Context, mPtr interface{}) error {
	if mA == nil {
		panic("mA not init")
	}
	return mA.DeleteEntityByID(ctx, mPtr)
}

func DeleteEntityByStringCondition(ctx context.Context, mPtr interface{}, cond string, args ...interface{}) error {
	if mA == nil {
		panic("mA not init")
	}
	return mA.DeleteEntityByStringCondition(ctx, mPtr, cond, args...)
}

func UpdateEntitySingleColumnByID(ctx context.Context, mPtr interface{}, column string, value interface{}) error {
	if mA == nil {
		panic("mA not init")
	}
	return mA.UpdateEntitySingleColumnByID(ctx, mPtr, column, value)
}

func GetTempCache(ctx context.Context, schemaName string, cond string) *utils.Cache {
	if mA == nil {
		panic("mA not init")
	}
	return mA.GetTempCache(ctx, schemaName, cond)
}

func QueryOneEntityByStringFilter(ctx context.Context, mPtr interface{}, cond string, args ...interface{}) error {
	if mA == nil {
		panic("mA not init")
	}
	return mA.QueryOneEntityByStringFilter(ctx, mPtr, cond, args...)
}

func QueryEntityListByStringCondition(ctx context.Context, modelListPtr interface{}, pageSize, page int, order string, desc bool, filter ...interface{}) (err error, total int) {
	if mA == nil {
		panic("mA not init")
	}
	return mA.QueryEntityListByStringCondition(ctx, modelListPtr, pageSize, page, order, desc, filter...)
}

func QueryEntityListByStructCondition(ctx context.Context, modelListPtr interface{},
	pageSize, page int, order string, desc bool, filter interface{}) (err error, total int) {
	if mA == nil {
		panic("mA not init")
	}
	return mA.QueryEntityListByStructCondition(ctx, modelListPtr, pageSize, page, order, desc, filter)
}

func ListSourceEntityRelations(ctx context.Context, query *EntityRelation,
	pageSize, page int, relationsInclude []string, filter map[string]map[string]string) (*RelationList, error) {
	if mA == nil {
		panic("mA not init")
	}
	return mA.ListSourceEntityRelations(ctx, query, pageSize, page, relationsInclude, filter)
}

func QueryRelationByUuid(ctx context.Context, filter *EntityRelation) (*EntityRelation, error) {
	return mA.QueryRelationByUuid(ctx, filter)
}

func UpdateRelationContentByID(ctx context.Context, relation *EntityRelation) (err error) {
	if mA == nil {
		panic("mA not init")
	}
	return mA.UpdateRelationContentByID(ctx, relation)
}

func DeleteRelation(ctx context.Context, relation *EntityRelation) (err error) {
	if mA == nil {
		panic("mA not init")
	}
	return mA.DeleteRelation(ctx, relation)
}

func CreateRelation(ctx context.Context, relation *EntityRelation) (err error) {
	if mA == nil {
		panic("mA not init")
	}
	return mA.CreateRelation(ctx, relation)
}

func WithTransaction(ctx context.Context, scopeDDLs txHandler) (err error) {
	if mA == nil {
		panic("mA not init")
	}
	return mA.WithTransaction(ctx, scopeDDLs)
}

func LockRecordByID(ctx context.Context, schema string, ids []int64) error {
	if mA == nil {
		panic("mA not init")
	}
	return mA.LockRecordByID(ctx, schema, ids)
}

func UpdateEntitySingleColumnByStringCondition(
	ctx context.Context, schema, column string, data interface{}, query string, args ...interface{}) error {
	if mA == nil {
		panic("mA not init")
	}
	return mA.UpdateEntitySingleColumnByStringCondition(ctx, schema, column, data, query, args...)
}

func UpdateEntityMultipleColumnByStringCondition(
	ctx context.Context, schema string, columns map[string]interface{}, query string, args ...interface{}) error {
	if mA == nil {
		panic("mA not init")
	}
	return mA.UpdateEntityMultipleColumnByStringCondition(ctx, schema, columns, query, args...)
}