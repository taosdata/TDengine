package sqlparser

import "testing"

func TestCreateDatabaseOption_CacheModelStringToken(t *testing.T) {
	stmt, err := Parse("create database if not exists db3 cachemodel 'none';")
	if err != nil {
		t.Fatalf("parse create database cachemodel failed: %v", err)
	}
	if _, ok := stmt.(*CreateDatabaseStmt); !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
}

func TestCreateDatabaseCompactInterval(t *testing.T) {
	stmt, err := Parse("create database if not exists db_ci compact_interval 30;")
	if err != nil {
		t.Fatalf("parse create database compact_interval failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.CompactInterval != 30 {
		t.Fatalf("unexpected compact_interval option: %+v", s.Options)
	}
}

func TestCreateDatabaseCompactTimeOffset(t *testing.T) {
	stmt, err := Parse("create database if not exists db_cto compact_time_offset 6;")
	if err != nil {
		t.Fatalf("parse create database compact_time_offset failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.CompactTimeOffset != 6 {
		t.Fatalf("unexpected compact_time_offset option: %+v", s.Options)
	}
}

func TestCreateDatabaseCompactTimeRange(t *testing.T) {
	stmt, err := Parse("create database if not exists db_ctr compact_time_range -10,20;")
	if err != nil {
		t.Fatalf("parse create database compact_time_range failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.CompactStartTime != -10 || s.Options.CompactEndTime != 20 {
		t.Fatalf("unexpected compact_time_range option: %+v", s.Options)
	}
}

func TestCreateDatabaseDefaultOptionsObject(t *testing.T) {
	stmt, err := Parse("create database if not exists db_default;")
	if err != nil {
		t.Fatalf("parse create database default options failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil {
		t.Fatalf("expected default options object, got nil")
	}
}

func TestCreateDatabaseDnodes(t *testing.T) {
	stmt, err := Parse("create database if not exists db_dns dnodes '1.1.1.1:6030';")
	if err != nil {
		t.Fatalf("parse create database dnodes failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.DnodeListStr != "1.1.1.1:6030" {
		t.Fatalf("unexpected dnodes option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_DurationIntegerNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db5 duration 7;")
	if err != nil {
		t.Fatalf("parse create database duration failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || len(s.Options.DaysPerFile) != 1 {
		t.Fatalf("unexpected database duration options: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_DurationVariableNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db5v duration 7d;")
	if err != nil {
		t.Fatalf("parse create database duration variable failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || len(s.Options.DaysPerFile) != 1 {
		t.Fatalf("unexpected database duration variable options: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_EncryptAlgorithmNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_enc encrypt_algorithm 'sm4';")
	if err != nil {
		t.Fatalf("parse create database encrypt_algorithm failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.EncryptAlgorithmStr != "sm4" {
		t.Fatalf("unexpected encrypt_algorithm option: %+v", s.Options)
	}
}

func TestCreateDatabaseIsAudit(t *testing.T) {
	stmt, err := Parse("create database if not exists db_ia is_audit 1;")
	if err != nil {
		t.Fatalf("parse create database is_audit failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.IsAudit != 1 {
		t.Fatalf("unexpected is_audit option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_KeepIntegerListNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_keep_i keep 1,2;")
	if err != nil {
		t.Fatalf("parse create database keep integer list failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || len(s.Options.Keep) != 2 {
		t.Fatalf("unexpected keep integer list options: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_KeepVariableListNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_keep_v keep 1d,2d;")
	if err != nil {
		t.Fatalf("parse create database keep variable list failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || len(s.Options.Keep) != 2 {
		t.Fatalf("unexpected keep variable list options: %+v", s.Options)
	}
}

func TestCreateDatabaseKeepTimeOffset(t *testing.T) {
	stmt, err := Parse("create database if not exists db_kto keep_time_offset 3;")
	if err != nil {
		t.Fatalf("parse create database keep_time_offset failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.KeepTimeOffset != 3 {
		t.Fatalf("unexpected keep_time_offset option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_KeepVariableNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_keep keep 1d;")
	if err != nil {
		t.Fatalf("parse create database keep variable failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || len(s.Options.Keep) != 1 {
		t.Fatalf("unexpected keep variable options: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_PrecisionStringToken(t *testing.T) {
	stmt, err := Parse("create database if not exists db2 precision 'ms';")
	if err != nil {
		t.Fatalf("parse create database precision failed: %v", err)
	}
	if _, ok := stmt.(*CreateDatabaseStmt); !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
}

func TestCreateDatabaseOption_Retentions(t *testing.T) {
	stmt, err := Parse("create database if not exists db6 retentions 1d:30d;")
	if err != nil {
		t.Fatalf("parse create database retentions failed: %v", err)
	}
	cd, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if cd.Options == nil || len(cd.Options.Retentions) == 0 {
		t.Fatalf("expected parsed retentions, got %+v", cd.Options)
	}
}

func TestCreateDatabaseOption_SchemalessNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_schema schemaless 1;")
	if err != nil {
		t.Fatalf("parse create database schemaless failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.Schemaless != 1 {
		t.Fatalf("unexpected schemaless option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_SSChunkPagesNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_sscp ss_chunkpages 64;")
	if err != nil {
		t.Fatalf("parse create database ss_chunkpages failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.SsChunkSize != 64 {
		t.Fatalf("unexpected ss_chunkpages option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_SSCompactNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_ss ss_compact 1;")
	if err != nil {
		t.Fatalf("parse create database ss_compact failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.SsCompact != 1 {
		t.Fatalf("unexpected ss_compact option: %+v", s.Options)
	}
}

func TestCreateDatabaseSsKeepLocal(t *testing.T) {
	stmt, err := Parse("create database if not exists db_sskl ss_keeplocal 11;")
	if err != nil {
		t.Fatalf("parse create database ss_keeplocal failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.SsKeepLocal != 11 {
		t.Fatalf("unexpected ss_keeplocal option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_STTTriggerNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_stt stt_trigger 8;")
	if err != nil {
		t.Fatalf("parse create database stt_trigger failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.SstTrigger != 8 {
		t.Fatalf("unexpected stt_trigger option: %+v", s.Options)
	}
}

func TestCreateDatabaseTablePrefix(t *testing.T) {
	stmt, err := Parse("create database if not exists db_tp table_prefix -7;")
	if err != nil {
		t.Fatalf("parse create database table_prefix failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.TablePrefix != -7 {
		t.Fatalf("unexpected table_prefix option: %+v", s.Options)
	}
}

func TestCreateDatabaseTablePrefixFloatRejected(t *testing.T) {
	if _, err := Parse("create database if not exists db_tp2 table_prefix 1.5;"); err == nil {
		t.Fatalf("expected parse error for float table_prefix")
	}
}

func TestCreateDatabaseTableSuffix(t *testing.T) {
	stmt, err := Parse("create database if not exists db_ts table_suffix +9;")
	if err != nil {
		t.Fatalf("parse create database table_suffix failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.TableSuffix != 9 {
		t.Fatalf("unexpected table_suffix option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_WalFsyncPeriodNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_fsync wal_fsync_period 100;")
	if err != nil {
		t.Fatalf("parse create database wal_fsync_period failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.fsyncPeriod != 100 {
		t.Fatalf("unexpected wal_fsync_period option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_WalLevel(t *testing.T) {
	stmt, err := Parse("create database if not exists db4 wal_level 1;")
	if err != nil {
		t.Fatalf("parse create database wal_level failed: %v", err)
	}
	if _, ok := stmt.(*CreateDatabaseStmt); !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
}

func TestCreateDatabaseWalRetentionPeriodNegative(t *testing.T) {
	stmt, err := Parse("create database if not exists db_wrp_neg wal_retention_period -3600;")
	if err != nil {
		t.Fatalf("parse create database wal_retention_period negative failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.WalRetentionPeriod != -3600 || !s.Options.WalRetentionPeriodIsSet {
		t.Fatalf("unexpected wal_retention_period option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_WalRetentionPeriodNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_wrp wal_retention_period 3600;")
	if err != nil {
		t.Fatalf("parse create database wal_retention_period failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.WalRetentionPeriod != 3600 || !s.Options.WalRetentionPeriodIsSet {
		t.Fatalf("unexpected wal_retention_period option: %+v", s.Options)
	}
}

func TestCreateDatabaseWalRetentionSizeNegative(t *testing.T) {
	stmt, err := Parse("create database if not exists db_wrs_neg wal_retention_size -4096;")
	if err != nil {
		t.Fatalf("parse create database wal_retention_size negative failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.WalRetentionSize != -4096 || !s.Options.WalRetentionSizeIsSet {
		t.Fatalf("unexpected wal_retention_size option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_WalRetentionSizeNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_wrs wal_retention_size 4096;")
	if err != nil {
		t.Fatalf("parse create database wal_retention_size failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.WalRetentionSize != 4096 || !s.Options.WalRetentionSizeIsSet {
		t.Fatalf("unexpected wal_retention_size option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_WalRollPeriodNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_wrp2 wal_roll_period 600;")
	if err != nil {
		t.Fatalf("parse create database wal_roll_period failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.WalRollPeriod != 600 || !s.Options.WalRollPeriodIsSet {
		t.Fatalf("unexpected wal_roll_period option: %+v", s.Options)
	}
}

func TestCreateDatabaseOption_WalSegmentSizeNoPanic(t *testing.T) {
	stmt, err := Parse("create database if not exists db_wss wal_segment_size 1024;")
	if err != nil {
		t.Fatalf("parse create database wal_segment_size failed: %v", err)
	}
	s, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if s.Options == nil || s.Options.WalSegmentSize != 1024 || !s.Options.WalSegmentSizeIsSet {
		t.Fatalf("unexpected wal_segment_size option: %+v", s.Options)
	}
}
