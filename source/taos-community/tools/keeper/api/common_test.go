package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_generateCreateDBSql(t *testing.T) {
	opts := map[string]interface{}{
		"precision": "ms",
		"replica":   2,
		"strict":    true,
	}
	sql := generateCreateDBSql("mydb", opts)
	assert.True(t, strings.HasPrefix(sql, "create database if not exists mydb"))

	cases := []string{
		" precision 'ms' ",
		" replica 2 ",
		" strict true ",
	}
	for _, sub := range cases {
		assert.Contains(t, sql, sub)
	}

	assert.True(t, strings.HasSuffix(sql, " "))
}
