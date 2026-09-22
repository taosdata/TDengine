package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/util"
)

func TestInsertSql_InjectionFieldsAreNeutralized(t *testing.T) {
	maliciousClusterID := "x (c) select col from otherdb.t --"
	maliciousEp := "e', 'c') values (now,'ready') otherdb.victim values (now, 1) d_info_z using d_info tags (1,'a','b"
	maliciousTs := "2026-09-15 00:00:00' OR '1'='1"

	suffix := util.ToValidTableName(maliciousClusterID + "1")

	// payloads containing single quotes must never appear verbatim in any statement
	assertPayloadNeutralized := func(sql string) {
		assert.NotContains(t, sql, maliciousEp)
		assert.NotContains(t, sql, maliciousTs)
		assert.NotContains(t, sql, "' OR '1'='1")
	}

	dnodeSql := insertDnodeSql(DnodeInfo{}, 1, maliciousEp, maliciousClusterID, maliciousTs)
	assert.True(t, strings.HasPrefix(dnodeSql, "insert into dnode_info_"+suffix+" using dnodes_info"))
	// dnode_ep is an nchar(255) tag and cluster_id an nchar(32) tag, so both are
	// truncated by runes (widths written out, not taken from the constants)
	assert.Contains(t, dnodeSql, "tags (1, '"+util.EscapeTruncRunes(maliciousEp, 255)+"', '"+util.EscapeTruncRunes(maliciousClusterID, 32)+"')")
	assert.Contains(t, dnodeSql, "values ('"+util.EscapeSQLString(maliciousTs)+"'")
	assertPayloadNeutralized(dnodeSql)

	sqls := insertClusterInfoSql(ClusterInfo{
		FirstEp: maliciousEp,
		Version: "3.4.0.0",
		Dnodes:  []Dnode{{DnodeID: 1, DnodeEp: maliciousEp, Status: "ready"}},
		Mnodes:  []Mnode{{MnodeID: 1, MnodeEp: maliciousEp, Role: "leader"}},
	}, maliciousClusterID, 1, maliciousTs)
	assert.Len(t, sqls, 3)
	assert.True(t, strings.HasPrefix(sqls[0], "insert into d_info_"+suffix+" "))
	assert.True(t, strings.HasPrefix(sqls[1], "insert into m_info_"+suffix+" "))
	assert.True(t, strings.HasPrefix(sqls[2], "insert into cluster_info_"+util.ToValidTableName(maliciousClusterID)+" "))
	for _, sql := range sqls {
		assertPayloadNeutralized(sql)
	}

	dataDirSqls := insertDataDirSql(DiskInfo{}, 1, maliciousEp, maliciousClusterID, maliciousTs)
	assert.Len(t, dataDirSqls, 2)
	for _, sql := range dataDirSqls {
		assertPayloadNeutralized(sql)
	}

	vgSqls := insertVgroupSql(VgroupInfo{DatabaseName: "db' --"}, 1, maliciousEp, maliciousClusterID, maliciousTs)
	for _, sql := range vgSqls {
		assert.NotContains(t, sql, "db' --")
		assertPayloadNeutralized(sql)
	}

	summarySql := insertLogSummary(LogInfo{}, 1, maliciousEp, maliciousClusterID, maliciousTs)
	assertPayloadNeutralized(summarySql)

	grantSql := insertGrantSql(GrantInfo{}, 1, maliciousClusterID, maliciousTs)
	assert.True(t, strings.HasPrefix(grantSql, "insert into grants_info_"+suffix+" "))
	assertPayloadNeutralized(grantSql)
}
