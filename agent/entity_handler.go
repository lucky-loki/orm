package agent

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/lucky-loki/orm/agent/utils"
	"log"
	"net/http"
	"strconv"
)

func registerEntityHandler(router gin.IRouter, ma *MetaAgent) {
	group := router.Group("/entity/:schema_name")
	group.POST("/create", createEntity(ma))
	group.PUT("/by/id/:id", updateEntity(ma))
	group.DELETE("/by/id/:id", deleteEntity(ma))
	group.GET("/by/id/:id", getEntityByID(ma))
	group.GET("/list", getEntityList(ma))
}

func createEntity(ma *MetaAgent) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 创建EntityModel
		schemaName := c.Param("schema_name")
		entity, exist := ma.GetModelPtr(schemaName)
		if !exist {
			failLog(c, "schema不存在: '%s'", schemaName)
			return
		}

		// 解析请求参数
		var err error
		if err = c.ShouldBindJSON(entity); err != nil {
			failLog(c, "解析请求失败")
			return
		}

		// 保存Entity
		err = ma.CreateEntity(context.Background(), entity)
		if err != nil {
			failLog(c, "数据保存失败: %s", err)
			return
		}
		success(c, nil)
	}
}

func updateEntity(ma *MetaAgent) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 解析ID
		var id int64
		var err error
		id, err = strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			failLog(c, "id 错误")
			return
		}

		// 创建EntityModel
		schemaName := c.Param("schema_name")
		entity, exist := ma.GetModelPtr(schemaName)
		if !exist {
			failLog(c, "schema不存在: '%s'", schemaName)
			return
		}

		// 解析请求参数
		if err = c.ShouldBindJSON(entity); err != nil {
			failLog(c, "解析请求失败")
			return
		}

		// 查询Entity
		var entityDB interface{}
		entityDB, _ = ma.GetModelPtr(schemaName)
		ctx := context.Background()
		err = ma.QueryOneEntityByStringFilter(ctx, entityDB, "id=?", id)
		if err != nil {
			failLog(c, "查找该业务失败: %s", err)
			return
		}

		// 更新Entity
		entity.(Schema).SetID(id)
		err = ma.UpdateEntityByID(ctx, entity)
		if err != nil {
			failLog(c, "更新业务失败: %s", err)
			return
		}
		success(c, nil)
	}
}

func deleteEntity(ma *MetaAgent) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 解析参数
		var err error
		var id int64
		id, err = strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			failLog(c, "id 错误")
			return
		}

		// 创建EntityModel
		schemaName := c.Param("schema_name")
		entityDB, exist := ma.GetModelPtr(schemaName)
		if !exist {
			failLog(c, "schema不存在: '%s'", schemaName)
			return
		}

		ctx := context.Background()
		err = ma.QueryOneEntityByStringFilter(ctx, entityDB, "id=?", id)
		if err != nil {
			failLog(c, "查询Entity失败: %s", err)
			return
		}
		// 删除Entity
		err = ma.DeleteEntityByID(ctx, entityDB)
		if err != nil {
			failLog(c, "删除Entity失败: %s", err)
			return
		}
		success(c, nil)
	}
}

type relationFilterParam struct {
	SourceSchemaName string                       `uri:"schema_name"`
	SourceEntityId   int64                        `uri:"id"`
	IncludeRelation  bool                         `form:"include_relation"`
	Relations        []string                     `form:"relations"`
	RelationPageSize int                          `form:"page_size"`
	RelationPage     int                          `form:"page"`
	RelationFilter   map[string]map[string]string `form:"relation_filter"`
}

type getEntityByIDResp struct {
	Entity          interface{} `json:"entity"`
	Relation        interface{} `json:"relation"`
	RelationContent interface{} `json:"relation_content"`
}

func getEntityByID(ma *MetaAgent) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 解析参数
		var err error
		var req relationFilterParam
		if err = c.ShouldBindUri(&req); err != nil {
			failLog(c, "解析参数出错: %s", err)
			return
		}
		if err = c.ShouldBindQuery(&req); err != nil {
			failLog(c, "解析参数出错: %s", err)
			return
		}

		// 创建EntityModel
		entityDB, exist := ma.GetModelPtr(req.SourceSchemaName)
		if !exist {
			failLog(c, "schema不存在: '%s'", req.SourceSchemaName)
			return
		}

		// 查询塞值
		var resp getEntityByIDResp
		ctx := context.Background()
		err = ma.QueryOneEntityByStringFilter(ctx, entityDB, "id=?", req.SourceEntityId)
		if err != nil {
			failLog(c, "查找Entity失败: %s", err)
			return
		}
		resp.Entity = entityDB

		// 查询relation
		if req.IncludeRelation {
			query := &EntityRelation{
				SourceSchemaName: req.SourceSchemaName,
				SourceEntityID:   req.SourceEntityId,
			}
			relationList, err := ma.ListSourceEntityRelations(ctx, query, req.RelationPageSize, req.RelationPage,
				req.Relations, req.RelationFilter)
			if err != nil {
				failLog(c, "查询关系出错: %s", err)
				return
			}
			resp.Relation = relationList.Relation
			resp.RelationContent = relationList.RelationContent
		}

		success(c, &resp)
	}
}

type getEntityListReq struct {
	SearchField string `form:"search_field"`
	Search      string `form:"search"`
	Page        int    `form:"page"`
	PageSize    int    `form:"page_size"`
}

type getEntityListResp struct {
	List  interface{} `json:"list"`
	Total int         `json:"total"`
}

func getEntityList(ma *MetaAgent) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 解析参数
		var err error
		var req getEntityListReq
		if err = c.ShouldBindQuery(&req); err != nil {
			failLog(c, "解析参数失败: %s", err)
			return
		}

		// 创建entityModel列表
		schemaName := c.Param("schema_name")
		list, exist := ma.GetModelListPtr(schemaName)
		if !exist {
			failLog(c, "schema 不存在: '%s'", schemaName)
			return
		}

		// 查询塞值
		var resp getEntityListResp
		ctx := context.Background()
		if req.SearchField != "" {
			filter, _ := ma.GetModelPtr(schemaName)
			err = utils.SetValueByTag(filter, req.SearchField, req.Search, "json")
			if err != nil {
				failLog(c, "search_field and search value error")
				return
			}
			err, resp.Total = ma.QueryEntityListByStructCondition(ctx, list,
				req.PageSize, req.Page, "id", true, filter)
		} else {
			err, resp.Total = ma.QueryEntityListByStringCondition(ctx, list,
				req.PageSize, req.Page, "id", true)
		}

		if err != nil {
			failLog(c, "查询EntityList失败: %s", err)
			return
		}
		resp.List = list
		success(c, &resp)
	}
}

// out put func

const (
	retOk    = 200
	retError = 1
)

func jsonOutPut(c *gin.Context, ret uint32, desc string, content interface{}) {
	result := map[string]interface{}{
		"code": ret,
		"msg":  desc,
	}
	if content != nil {
		result["data"] = content
	}
	c.JSON(http.StatusOK, result)
}

func success(c *gin.Context, content interface{}) {
	jsonOutPut(c, retOk, "success", content)
}

func failLog(c *gin.Context, format string, a ...interface{}) {
	var desc string
	if len(a) > 0 {
		desc = fmt.Sprintf(format, a...)
	} else {
		desc = format
	}
	log.Println(desc)
	jsonOutPut(c, retError, desc, nil)
}
