package querier

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func userWithID(id int) QueryBuilderOption {
	return func(query *Query) {
		query.where = append(query.where, "users.id = ?")
		query.params = append(query.params, id)
	}
}

func setUserName(name string) QueryBuilderOption {
	return func(query *Query) {
		query.sets = append(query.sets, "name")
		query.params = append(query.params, name)
	}
}

func TestNewQuery(t *testing.T) {
	var (
		users          DBTable = "users"
		products       DBTable = "products"
		userID         DBField = "users.id"
		productsUserID DBField = "products.user_id"
		userName       DBField = "users.name"
	)

	t.Run("select all", func(t *testing.T) {
		query, params := NewQuery(users, nil)
		require.Equal(t, "SELECT * FROM users", query)
		require.Empty(t, params)
	})

	t.Run("select from ids", func(t *testing.T) {
		ids := []int{1, 2, 3}
		query, params := NewQuery(users, []DBField{userName}, Where(userID, In, toAnySlice(ids)...))

		require.Equal(t, "SELECT users.name FROM users WHERE users.id IN (?,?,?)", query)
		require.Len(t, params, 3)
		require.Equal(t, 1, params[0])
	})

	t.Run("select specific fields", func(t *testing.T) {
		query, params := NewQuery(users, []DBField{userID})
		require.Equal(t, "SELECT users.id FROM users", query)
		require.Empty(t, params)
	})

	t.Run("select with condition", func(t *testing.T) {
		query, params := NewQuery(users, []DBField{userID}, userWithID(123))
		require.Equal(t, "SELECT users.id FROM users WHERE users.id = ?", query)
		require.Len(t, params, 1)
		require.Equal(t, 123, params[0])
	})

	t.Run("select with aggregation", func(t *testing.T) {
		query, params := NewQuery(users, nil, Limit(1), OrderBy(userID, Desc))
		require.Equal(t, "SELECT * FROM users LIMIT ? ORDER BY users.id DESC", query)
		require.Len(t, params, 0)
	})

	t.Run("join tables without condition", func(t *testing.T) {
		query, params := NewQuery(users, nil, Join(products, InnerJoin, "", ""))
		require.Equal(t, "SELECT * FROM users INNER JOIN products", query)
		require.Empty(t, params)
	})

	t.Run("join tables with condition", func(t *testing.T) {
		query, params := NewQuery(users, nil, Join(products, LeftJoin, userID, productsUserID))
		require.Equal(t, "SELECT * FROM users LEFT JOIN products ON users.id = products.user_id", query)
		require.Empty(t, params)
	})

	t.Run("fields from tables", func(t *testing.T) {
		query, params := NewQuery(users, nil, Join(products, LeftJoin, userID, productsUserID))
		require.Equal(t, "SELECT * FROM users LEFT JOIN products ON users.id = products.user_id", query)
		require.Empty(t, params)
	})

	t.Run("where with joined tables", func(t *testing.T) {
		query, params := NewQuery(users, []DBField{userID, productsUserID}, Join(products, RightJoin, userID, productsUserID))
		require.Equal(t, "SELECT users.id, products.user_id FROM users RIGHT JOIN products ON users.id = products.user_id", query)
		require.Empty(t, params)
	})
}

func TestNewDelete(t *testing.T) {
	var (
		users DBTable = "users"
	)

	t.Run("delete rows", func(t *testing.T) {
		query, params := NewDelete(users, userWithID(123))
		require.Equal(t, "DELETE FROM users WHERE users.id = ?", query)
		require.Len(t, params, 1)
		require.Equal(t, 123, params[0])
	})
}

func TestNewUpdate(t *testing.T) {
	var (
		users DBTable = "users"
	)

	t.Run("update fields", func(t *testing.T) {
		query, params := NewUpdate(users, setUserName("bla"), userWithID(2))
		require.Equal(t, "UPDATE users SET name = ? WHERE users.id = ?", query)
		require.Len(t, params, 2)
		require.Equal(t, "bla", params[0])
		require.Equal(t, 2, params[1])
	})
}

func TestNewInsert(t *testing.T) {
	var (
		users DBTable = "users"

		name    DBField = "name"
		address DBField = "address"
		status  DBField = "status"
	)

	res := NewInsert(users, []DBField{name, address, status})
	require.Equal(t, "INSERT INTO users (name, address, status) VALUES (?, ?, ?)", res)
}

func toAnySlice[T any](s []T) []any {
	result := make([]any, len(s))
	for i, v := range s {
		result[i] = v
	}
	return result
}
