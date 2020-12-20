package agent

// 包装事务处理流程，方便进行更好的dao逻辑组织

import (
	"context"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"sort"
)

type txKey struct{}

// 实现的txHandler将一组ddl函数组合起来，返回值表示这一组ddl执行是否成功
// 可以通过 `GetTxConnFromContext` 从ctx中获取已经开启事务的连接
type txHandler func(ctx context.Context) error

// 从ctx获取已经开启事务的连接
func (ma *MetaAgent) getTxConnFromContext(ctx context.Context) (*gorm.DB, error) {
	if tx, ok := ctx.Value(txKey{}).(*gorm.DB); ok {
		return tx, nil
	}
	return nil, errors.New("tx conn not found")
}

// 包装事务，可以更优雅的组织事务代码，支持mini-tx
func (ma *MetaAgent) WithTransaction(ctx context.Context, scopeDDLs txHandler) (err error) {
	// 功能: 包装事务，dao层不需要关系使用的db conn是否是开启事务的，从而可以更加优雅的组织dao的代码
	//		1. 从ctx获取事务连接，如果没有则主动开启事务并将parent设置为true
	//		2. 执行由ddlsFunc封装的一组ddl
	// 		3. 如果ddlsFunc panic或者返回err，那么事务将执行回滚
	//		4. 如果ddlsFunc返回nil，就认为这个mini-tx执行成功
	//		5. 最后由parent提交事务

	// 获取事务连接，如果没有则开启事务
	var tx *gorm.DB
	var parent bool // 事务由parent提交
	tx, err = ma.getTxConnFromContext(ctx)
	if err != nil {
		tx = ma.db.Begin()
		parent = true
	}

	err = tx.Error
	if err != nil {
		return
	}
	defer func() {
		// scopeDDLFunc触发panic，回滚
		if p := recover(); p != nil {
			tx.Rollback()
			err = fmt.Errorf("tx panic: %v", p)
			return
		}
		// scopeDDLFunc执行出错，回滚
		if err != nil {
			tx.Rollback()
			return
		}
		// 非parent直接退出不提交
		if !parent {
			return
		}
		// 提交
		err = tx.Commit().Error
		if err != nil {
			return
		}
	}()
	newCtx := context.WithValue(ctx, txKey{}, tx)
	// 执行一组ddl，如果返回nil视为这组ddl表示的mini-tx执行成功准备提交
	err = scopeDDLs(newCtx)
	return
}

func (ma *MetaAgent) LockRecordByID(ctx context.Context, schema string, ids []int64) error {
	db := ma.GetDB(ctx)
	intIds := make([]int, len(ids))
	for i, id := range ids {
		intIds[i] = int(id)
	}
	sort.Ints(intIds)
	sql := "select * from " + schema + " where id in (?) for update"
	err :=  db.Exec(sql, ids).Error
	return err
}