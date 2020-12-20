package agent

import (
	"context"
	"errors"
	"github.com/lucky-loki/orm/agent/utils"
	"time"
)

type Checker interface {
	Check() error
}

type SoftDeleter interface {
	SoftDelete()
}

type Entity struct {
	ID        int64     `json:"id" gorm:"primary_key;auto_increment"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CreateEntity 向数据库插入数据，保存所有列数据，ID由数据库自动生成
// 		insert into {schema_name}
//			(column1, column2, ...)
//		values
//			({value1}, {value2}, ...)
func (ma *MetaAgent) CreateEntity(ctx context.Context, mPtr interface{}) error {
	var err error
	if c, ok := mPtr.(Checker); ok {
		if err = c.Check(); err != nil {
			return err
		}
	}
	if s, ok := mPtr.(Schema); ok {
		s.SetID(0)
	}
	db := ma.GetDB(ctx)
	return db.Create(mPtr).Error
}

// UpdateEntityByID 全量更新，如果某字段为空就意味着更新为空值
//	sql like:
//		update {schema_name}
//		set column1={value1}, column2={value2},...
//		where id={id}
func (ma *MetaAgent) UpdateEntityByID(ctx context.Context, mPtr interface{}) error {
	db := ma.GetDB(ctx)
	return db.Save(mPtr).Error
}

func (ma *MetaAgent) UpdateEntitySingleColumnByStringCondition(
	ctx context.Context, schema, column string, data interface{}, query string, args ...interface{}) error {
	db := ma.GetDB(ctx)
	err := db.Table(schema).Where(query, args...).Update(column, data).Error
	return err
}

func (ma *MetaAgent) UpdateEntityMultipleColumnByStringCondition(
	ctx context.Context, schema string, columns map[string]interface{}, query string, args ...interface{}) error {
	db := ma.GetDB(ctx)
	err := db.Table(schema).Where(query, args...).Updates(columns).Error
	return err
}

// DeleteEntityByID 通过ID删除数据
//	如果model实现了软删除则执行update sql，否则执行delete sql
//  sql like:
//		update {schema_name}
//		set column1={value1}, column2={value2}, ...
//		where id={id}
//	or
//		delete from schema_name
//		where id={id}
func (ma *MetaAgent) DeleteEntityByID(ctx context.Context, mPtr interface{}) error {
	if d, ok := mPtr.(SoftDeleter); ok {
		d.SoftDelete()
		return ma.UpdateEntityByID(ctx, mPtr)
	}
	db := ma.GetDB(ctx)
	return db.Unscoped().Delete(mPtr).Error
}

// DeleteEntityByStringCondition 条件删除
func (ma *MetaAgent) DeleteEntityByStringCondition(ctx context.Context, mPtr interface{}, cond string, args ...interface{}) error {
	db := ma.GetDB(ctx)
	return db.Unscoped().Where(cond, args...).Delete(mPtr).Error
}

// UpdateEntitySingleColumnByID 通过ID更新一个指定列
//	sql like:
//		update {schema_name}
//		set {column}={value}
//		where id={id}
func (ma *MetaAgent) UpdateEntitySingleColumnByID(ctx context.Context, mPtr interface{}, column string, value interface{}) error {
	db := ma.GetDB(ctx)
	return db.Model(mPtr).Update(column, value).Error
}

// GetTempCache 获取一个临时的查询缓存，不保证查询结果与最新的数据库结果一致
func (ma *MetaAgent) GetTempCache(ctx context.Context, schemaName string, cond string) *utils.Cache {
	var cache utils.Cache
	cache.GetFromOrigin = func(key interface{}) (interface{}, error) {
		mPtr, ok := ma.GetModelPtr(schemaName)
		if !ok {
			return nil, errors.New("schema not support")
		}
		err := ma.QueryOneEntityByStringFilter(ctx, mPtr, cond, key)
		return mPtr, err
	}
	return &cache
}

// GetEntityByID 根据主键查询Entity
func (ma *MetaAgent) QueryEntity(ctx context.Context, mPtr interface{}) error {
	db := ma.GetDB(ctx)
	return db.Find(mPtr).Error
}

// QueryOneEntityByStringFilter 通过过滤条件查找一条数据，如果记录不存在则返回error
//	sql like:
//		select (column1, column2,...) from {schema_name}
//		where {where...}
func (ma *MetaAgent) QueryOneEntityByStringFilter(ctx context.Context, mPtr interface{}, cond string, args ...interface{}) error {
	db := ma.GetDB(ctx)
	return db.Where(cond, args...).First(mPtr).Error
}

// QueryOneEntityByStructFilter 通过过滤条件查找一条数据
//	sql like:
//		select (column1, column2,...) from {schema_name}
//		where {where...}
func (ma *MetaAgent) QueryOneEntityByStructFilter(ctx context.Context, mPtr interface{}, filter interface{}) error {
	db := ma.GetDB(ctx)
	return db.Where(filter).First(mPtr).Error
}

// QueryEntityListByStringCondition 通过过滤条件进行分页查询，
//	如果pageSize为0则变更成全量查询且忽视page值
//	sql like:
//		select (column1, column2,...) from {schema_name}
//		where {where...}
//		[order by {column} [desc]]
//		[offset {pageSize * (page - 1)} limit {pageSize}]
func (ma *MetaAgent) QueryEntityListByStringCondition(ctx context.Context, modelListPtr interface{}, pageSize, page int, order string, desc bool, filter ...interface{}) (err error, total int) {
	db := ma.GetDB(ctx)
	// 添加过滤条件
	if len(filter) > 0 {
		db = db.Where(filter[0], filter[1:]...)
	}
	// 检查数据库是否有数据
	err = db.Find(modelListPtr).Count(&total).Error
	if err != nil || total == 0 {
		return
	}
	// 分页查询
	if pageSize != 0 {
		if page == 0 {
			page = 1
		}
		offset := pageSize * (page - 1)

		db = db.Limit(pageSize).Offset(offset)
	}
	//是否排序
	if order != "" {
		if desc {
			order = order + " desc"
		}
		db = db.Order(order, true)
	}
	err = db.Find(modelListPtr).Error
	return
}

// QueryEntityListByStructCondition 通过struct过滤条件进行分页查询，
//	如果pageSize为0则变更成全量查询且忽视page值
//	sql like:
//		select (column1, column2,...) from {schema_name}
//		where {filter...}
//		[order by {column} [desc]]
//		[offset {pageSize * (page - 1)} limit {pageSize}]
func (ma *MetaAgent) QueryEntityListByStructCondition(ctx context.Context, modelListPtr interface{}, pageSize, page int, order string, desc bool, filter interface{}) (err error, total int) {
	db := ma.GetDB(ctx)
	// 添加过滤条件
	if filter != nil {
		db = db.Where(filter)
	}
	// 检查数据库是否有数据
	err = db.Find(modelListPtr).Count(&total).Error
	if err != nil || total == 0 {
		return
	}
	// 分页查询
	if pageSize != 0 {
		if page == 0 {
			page = 1
		}
		offset := pageSize * (page - 1)

		db = db.Limit(pageSize).Offset(offset)
	}
	//是否排序
	if order != "" {
		if desc {
			order = order + " desc"
		}
		db = db.Order(order, true)
	}
	err = db.Find(modelListPtr).Error
	return
}
