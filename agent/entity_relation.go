package agent

import (
	"context"
	"errors"
)

const (
	EntityRelationStatusENABLE  = "ENABLE"
	EntityRelationStatusDISABLE = "DISABLE"
)

var _ Schema = &EntityRelation{}

type EntityRelation struct {
	Entity

	SourceSchemaName string `uri:"source_schema_name" json:"source_schema_name" gorm:"unique_index:uuid"`
	SourceEntityID   int64  `uri:"source_entity_id" json:"source_entity_id" gorm:"unique_index:uuid"`
	TargetSchemaName string `uri:"target_schema_name" json:"target_schema_name" gorm:"unique_index:uuid"`
	TargetEntityID   int64  `uri:"target_entity_id" json:"target_entity_id" gorm:"unique_index:uuid"`
	Content          string `json:"content" gorm:"type:blob"`
}

func (er *EntityRelation) SchemaName() string {
	return "entity_relation"
}

func (er *EntityRelation) NewFunc() interface{} {
	return &EntityRelation{}
}

func (er *EntityRelation) NewListFunc() interface{} {
	var list []*EntityRelation
	return &list
}

func (er *EntityRelation) GetID() int64 {
	return er.ID
}

func (er *EntityRelation) SetID(id int64) {
	er.ID = id
}

// CheckRelation 检查relation是否合规
func (relation *EntityRelation) Check(ctx context.Context, ma *MetaAgent) error {
	if relation == nil {
		return errors.New("relation can not be nil")
	}
	// 检查schema是否被注册
	if _, exist := ma.pool[relation.SourceSchemaName]; !exist {
		return errors.New("schema not register: " + relation.SourceSchemaName)
	}
	if _, exist := ma.pool[relation.TargetSchemaName]; !exist {
		return errors.New("schema not register: " + relation.TargetSchemaName)
	}

	// 检查Entity是否存在
	entityPtr, _ := ma.GetModelPtr(relation.SourceSchemaName)
	entityPtr.(Schema).SetID(relation.SourceEntityID)
	err := ma.QueryEntity(ctx, entityPtr)
	if err != nil {
		return err
	}
	entityPtr, _ = ma.GetModelPtr(relation.TargetSchemaName)
	entityPtr.(Schema).SetID(relation.TargetEntityID)
	err = ma.QueryEntity(ctx, entityPtr)
	return err
}

// AddRelation 检查relation是否已经注册，如果检查通过则插入一条relation
//	sql like:
//		insert into entity_relation
//		(source_schema_name, source_entity_id, target_schema_name, target_entity_id, content)
//		value
//		({source_schema_name}, {source_entity_id}, {target_schema_name}, {target_entity_id}, {content})
func (ma *MetaAgent) CreateRelation(ctx context.Context, relation *EntityRelation) (err error) {
	return ma.CreateEntity(ctx, relation)
}

func checkListSourceEntityRelationsQuery(ctx context.Context, q *EntityRelation, ma *MetaAgent) error {
	if q.SourceSchemaName == "" || q.SourceEntityID == 0 {
		return errors.New("source_schema_name and source_entity_id cannot be empty")
	}
	exist := false
	// 检验schema是否被注册
	if _, exist = ma.pool[q.SourceSchemaName]; !exist {
		return errors.New("schema not register: " + q.SourceSchemaName)
	}

	if q.TargetSchemaName != "" {
		_, exist = ma.pool[q.TargetSchemaName]
		if !exist {
			return errors.New("schema not register: " + q.TargetSchemaName)
		}
	}

	// 检验entity是否存在
	entityPtr, _ := ma.GetModelPtr(q.SourceSchemaName)
	err := ma.QueryEntity(ctx, entityPtr)
	return err
}

type RelationList struct {
	Relation        map[string]interface{}      `json:"relation"`
	RelationContent map[string]map[int64]string `json:"relation_content"`
}

// ListRelation 列出source_schema的relation
//	sql like:
//	  query entity_relation
//		select (column1, column2,...) from entity_relation
//		where
//		source_schema_name={source_schema_name} and source_entity_id={source_entity_id}
//		[ and target_schema_name={target_schema_name}
//			[and target_entity_id={target_entity_id} ] ]
//
//	  loop res.target_schemas query entity
//		select (column1, column2,...) from {target_schema_name}
//		where id in (?)
//		[ and {filter} ]
//		[ limit [pageSize] offset {pageSize*(page-1)} ]
func (ma *MetaAgent) ListSourceEntityRelations(ctx context.Context, query *EntityRelation,
	pageSize, page int, relationsInclude []string, filter map[string]map[string]string) (*RelationList, error) {
	var err error
	if err = checkListSourceEntityRelationsQuery(ctx, query, ma); err != nil {
		return nil, err
	}
	// 检查relationsInclude是否有没注册的

	// 查询EntityRelation
	var total int
	var relationList []*EntityRelation
	err, total = ma.QueryEntityListByStructCondition(ctx, &relationList, 0, 1, "id", true, query)
	if err != nil {
		return nil, err
	}
	if total == 0 {
		return nil, nil
	}

	var targetIds = make(map[string][]int64)
	var relationContent = make(map[string]map[int64]string)
	var res RelationList
	targetSchemas := make([]string, 0)
	for _, r := range relationList {
		targetSchemas = append(targetSchemas, r.TargetSchemaName)
		targetIds[r.TargetSchemaName] = append(targetIds[r.TargetSchemaName], r.TargetEntityID)
		_, exist := relationContent[r.TargetSchemaName]
		if !exist {
			relationContent[r.TargetSchemaName] = map[int64]string{}
		}
		relationContent[r.TargetSchemaName][r.TargetEntityID] = r.Content
	}
	res.RelationContent = relationContent
	// 如果不指定包含的relation，将返回已有的所有relation
	if len(relationsInclude) == 0 {
		relationsInclude = targetSchemas
	}

	var args []interface{}
	var relations = make(map[string]interface{})
	for _, targetSchema := range relationsInclude {
		// 如果还没有relation则跳过
		ids := targetIds[targetSchema]
		if len(ids) == 0 {
			continue
		}

		args = args[:0]
		// 构建query参数
		cond := "id in (?)"
		args = append(args, cond)
		args = append(args, ids)
		for field, value := range filter[targetSchema] {
			cond = cond + " and " + field + "=?"
			args = append(args, value)
		}
		args[0] = cond

		// 查询
		entityListPtr, _ := ma.GetModelListPtr(targetSchema)
		err, _ = ma.QueryEntityListByStringCondition(ctx, entityListPtr, pageSize, page, "id", false, args...)
		if err != nil {
			return nil, err
		}
		relations[targetSchema] = entityListPtr
	}
	res.Relation = relations
	return &res, nil
}

func (ma *MetaAgent) QueryRelationByUuid(ctx context.Context, filter *EntityRelation) (*EntityRelation, error) {
	var relation []*EntityRelation
	err, _ := ma.QueryEntityListByStructCondition(ctx, &relation, 0, 1, "", false, filter)
	if err != nil {
		return nil, err
	}
	if len(relation) == 1 {
		return relation[0], nil
	}
	return nil, errors.New("uuid not exist")
}

// 更新relation content
func (ma *MetaAgent) UpdateRelationContentByID(ctx context.Context, relation *EntityRelation) (err error) {
	content := relation.Content
	if relation.ID == 0 {
		relation.Content = ""
		relation, err = ma.QueryRelationByUuid(ctx, relation)
		if err != nil {
			return err
		}
	}
	return ma.UpdateEntitySingleColumnByID(ctx, relation, "content", content)
}

// 删除relation
func (ma *MetaAgent) DeleteRelation(ctx context.Context, relation *EntityRelation) (err error) {
	if relation.ID == 0 {
		relation.Content = ""
		relation, err = ma.QueryRelationByUuid(ctx, relation)
		if err != nil {
			return err
		}
	}
	return ma.DeleteEntityByID(ctx, relation)
}
