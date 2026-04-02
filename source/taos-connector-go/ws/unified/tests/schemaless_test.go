package tests

import (
	"database/sql/driver"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnifiedIntegrationSchemaless_OpenWithDSN(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}

	client := openUnifiedIntegrationClient(t)
	t.Cleanup(func() { client.Close() })

	dbName := fmt.Sprintf("unified_it_sml_%d", time.Now().UnixNano())
	measurement := "sml_all_types"
	_, err := client.Exec(0, fmt.Sprintf("create database if not exists %s", dbName))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err = client.Exec(0, fmt.Sprintf("drop database if exists %s", dbName))
		assert.NoError(t, err)
	})
	_, err = client.Exec(0, fmt.Sprintf("use %s", dbName))
	require.NoError(t, err)

	lines := strings.Join([]string{
		measurement + ",site=s1 v=11i 1711111111000",
		measurement + ",site=s2 v=22i 1711111112000",
		measurement + ",site=s3 v=33i 1711111113000",
	}, "\n")
	err = client.SchemalessInsert(0, lines, 1, "ms", 0, "")
	require.NoError(t, err)

	rows, err := client.Query(0, fmt.Sprintf("select v from %s.%s", dbName, measurement))
	require.NoError(t, err)
	require.NotNil(t, rows)
	t.Cleanup(func() { _ = rows.Close() })

	got := make([]string, 0, 3)
	for {
		dest := make([]driver.Value, 1)
		nextErr := rows.Next(dest)
		if nextErr == io.EOF {
			break
		}
		require.NoError(t, nextErr)
		got = append(got, fmt.Sprint(dest[0]))
	}
	sort.Strings(got)
	require.Equal(t, []string{"11", "22", "33"}, got)
}
