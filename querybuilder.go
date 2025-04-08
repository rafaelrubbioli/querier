package querier

import (
	"fmt"
	"strings"
)

type DBTable string

type DBField string

const (
	Count DBField = "COUNT(*)"
)

type QueryOperation string

const (
	Insert QueryOperation = "INSERT INTO"
	Select QueryOperation = "SELECT"
	Update QueryOperation = "UPDATE"
	Delete QueryOperation = "DELETE"
)

type JoinType string

const (
	InnerJoin JoinType = "INNER"
	LeftJoin  JoinType = "LEFT"
	RightJoin JoinType = "RIGHT"
	CrossJoin JoinType = "CROSS"
	FullJoin  JoinType = "FULL"
)

type OrderByType string

const (
	Desc OrderByType = "DESC"
	ASC  OrderByType = "ASC"
)

type DBOperation string

const (
	NotEqual       DBOperation = "NOT EQUAL"
	Equal          DBOperation = "="
	LessOrEqual    DBOperation = "<="
	LessThan       DBOperation = "<"
	GreaterOrEqual DBOperation = ">="
	GreaterThan    DBOperation = ">"
	NotInt         DBOperation = "NOT IN"
	In             DBOperation = "IN"
	Like           DBOperation = "LIKE"
)

type Query struct {
	Table  DBTable
	fields []DBField
	where  []string
	params []any
	join   []string

	aggregations      []string
	aggregationParams []any

	sets      []string
	setParams []any
}

type QueryBuilderOption func(query *Query)

func NewQuery(table DBTable, fields []DBField, opts ...QueryBuilderOption) (string, []any) {
	query := &Query{Table: table, params: make([]any, 0), aggregationParams: make([]any, 0)}
	for _, opt := range opts {
		opt(query)
	}

	res := fmt.Sprint(Select)
	if len(fields) == 0 {
		res += " *"
	}

	for i, w := range fields {
		res += " " + string(w)
		if i != len(fields)-1 {
			res += ","
		}
	}

	res += " FROM"
	res += fmt.Sprintf(" %s", table)

	for _, join := range query.join {
		res += join
	}

	resultParams := make([]any, 0, len(query.params)+len(query.aggregations))
	if len(query.where) > 0 {
		res += " WHERE "
		for i, w := range query.where {
			res += w

			if i != len(query.where)-1 {
				res += " AND "
			}
		}
	}
	resultParams = append(resultParams, query.params...)

	if len(query.aggregations) > 0 {
		res += " "
		for i, ag := range query.aggregations {
			res += ag

			if i != len(query.aggregations)-1 {
				res += " "
			}
		}
	}
	resultParams = append(resultParams, query.aggregationParams...)

	return res, query.params
}

func NewInsert(table DBTable, fields []DBField) string {
	res := fmt.Sprintf("%s %s (", Insert, table)
	values := " VALUES ("
	for i, w := range fields {
		res += strings.Replace(string(w), string(table)+".", "", 11)
		values += "?"
		if i != len(fields)-1 {
			res += ", "
			values += ", "
		}
	}

	res += ")" + values + ")"

	return res
}

func NewUpdate(table DBTable, opts ...QueryBuilderOption) (string, []any) {
	query := &Query{Table: table, params: make([]any, 0), aggregationParams: make([]any, 0), setParams: make([]any, 0)}
	for _, opt := range opts {
		opt(query)
	}

	res := fmt.Sprint(Update)
	res += fmt.Sprintf(" %s", table)

	resultParams := make([]any, 0, len(query.params)+len(query.setParams)+len(query.aggregations))
	res += " SET"
	for i, w := range query.sets {
		res += " " + strings.Replace(string(w), string(table)+".", "", 1) + " = ?"
		if i != len(query.sets)-1 {
			res += ","
		}
	}
	resultParams = append(resultParams, query.setParams...)

	for _, join := range query.join {
		res += join
	}

	if len(query.where) > 0 {
		res += " WHERE "
		for i, w := range query.where {
			res += w
			if i != len(query.where)-1 {
				res += " AND "
			}
		}
	}
	resultParams = append(resultParams, query.params...)

	if len(query.aggregations) > 0 {
		res += " "
		for i, ag := range query.aggregations {
			res += ag

			if i != len(query.aggregations)-1 {
				res += " "
			}
		}
	}
	resultParams = append(resultParams, query.aggregationParams...)

	return res, resultParams
}

func NewDelete(table DBTable, opts ...QueryBuilderOption) (string, []any) {
	query := &Query{Table: table, params: make([]any, 0), aggregationParams: make([]any, 0)}
	for _, opt := range opts {
		opt(query)
	}

	res := fmt.Sprint(Delete)
	res += " FROM"
	res += fmt.Sprintf(" %s", table)

	resultParams := make([]any, 0, len(query.params)+len(query.aggregations))
	for _, join := range query.join {
		res += join
	}

	if len(query.where) > 0 {
		res += " WHERE "
		for i, w := range query.where {
			res += w
			if i != len(query.where)-1 {
				res += " AND "
			}
		}
	}
	resultParams = append(resultParams, query.params...)

	if len(query.aggregations) > 0 {
		res += " "
		for i, ag := range query.aggregations {
			res += ag

			if i != len(query.aggregations)-1 {
				res += " "
			}
		}
	}
	resultParams = append(resultParams, query.aggregationParams...)

	return res, query.params
}

func Where(field DBField, operation DBOperation, params ...any) QueryBuilderOption {
	return func(q *Query) {
		where := q.buildWhere(field, operation, params)
		q.where = append(q.where, where)
	}
}

func And(opts ...QueryBuilderOption) QueryBuilderOption {
	return func(q *Query) {
		for _, opt := range opts {
			opt(q)
		}
	}
}

func Or(opts ...QueryBuilderOption) QueryBuilderOption {
	return func(q *Query) {
		temp := &Query{}
		for _, opt := range opts {
			opt(temp)
		}

		where := ""
		for i, w := range temp.where {
			where += w
			if i != len(temp.where)-1 {
				where += " OR "
			}
		}

		q.where = append(q.where, where)
		q.params = append(q.params, temp.params...)
	}
}

func Set(field DBField, value any) QueryBuilderOption {
	return func(q *Query) {
		q.sets = append(q.sets, string(field))
		q.setParams = append(q.setParams, value)
	}
}

func Raw(query string, params ...any) QueryBuilderOption {
	return func(q *Query) {
		q.aggregations = append(q.aggregations, query)
		q.aggregationParams = append(q.aggregationParams, params...)
	}
}

func RawWhere(query string, params ...any) QueryBuilderOption {
	return func(q *Query) {
		q.where = append(q.where, query)
		q.params = append(q.params, params...)
	}
}

func Join(table DBTable, joinType JoinType, on, equal DBField) QueryBuilderOption {
	return func(query *Query) {
		join := fmt.Sprintf(" %s JOIN %s", joinType, table)
		if on != "" && equal != "" {
			join += fmt.Sprintf(" ON %s = %s", on, equal)
		}
		query.join = append(query.join, join)
	}
}

func Limit(limit int) QueryBuilderOption {
	return func(query *Query) {
		query.aggregations = append(query.aggregations, "LIMIT ?")
		query.aggregationParams = append(query.aggregationParams, limit)
	}
}

func OrderBy(field DBField, order OrderByType) QueryBuilderOption {
	return func(query *Query) {
		query.aggregations = append(query.aggregations, fmt.Sprintf("ORDER BY %s %s", field, order))
	}
}

func First() QueryBuilderOption {
	return func(query *Query) {
		query.aggregations = append(query.aggregations, "LIMIT 1")
	}
}

func (q *Query) buildWhere(field DBField, operation DBOperation, params []any) string {
	where := fmt.Sprintf("%s %s", field, operation)

	switch {
	case len(params) == 1:
		where += " ?"
		q.params = append(q.params, params[0])

	case len(params) > 1:
		where += " ("

		for i, param := range params {
			where += "?"
			q.params = append(q.params, param)

			if i != len(params)-1 {
				where += ","
			}
		}

		where += ")"
	}

	return where
}
