package sqlparser

const (
	PRIV_TYPE_UNKNOWN = -1
	// ==================== Common Privilege ====================
	PRIV_CM_ALL         = 0  // ALL PRIVILEGES
	PRIV_CM_ALTER       = 1  // ALTER PRIVILEGE
	PRIV_CM_DROP        = 2  // DROP PRIVILEGE
	PRIV_CM_SHOW        = 3  // SHOW PRIVILEGE
	PRIV_CM_SHOW_CREATE = 4  // SHOW CREATE PRIVILEGE
	PRIV_CM_START       = 5  // START PRIVILEGE
	PRIV_CM_STOP        = 6  // STOP PRIVILEGE
	PRIV_CM_RECALC      = 7  // RECALC PRIVILEGE
	PRIV_CM_KILL        = 8  // KILL PRIVILEGE
	PRIV_CM_SUBSCRIBE   = 9  // SUBSCRIBE PRIVILEGE
	PRIV_CM_MAX         = 29 // MAX COMMON PRIVILEGE

	// ==================== DB Privileges(5~49) ====================
	PRIV_DB_CREATE     = 30 // CREATE DATABASE
	PRIV_DB_DROP_OWNED = 31 // DROP OWNED DATABASE
	PRIV_DB_USE        = 32 // USE DATABASE
	PRIV_DB_FLUSH      = 33 // FLUSH DATABASE
	PRIV_DB_COMPACT    = 34 // COMPACT DATABASE
	PRIV_DB_TRIM       = 35 // TRIM DATABASE
	PRIV_DB_ROLLUP     = 36 // ROLLUP DATABASE
	PRIV_DB_SCAN       = 37 // SCAN DATABASE
	PRIV_DB_SSMIGRATE  = 38 // SSMIGRATE DATABASE

	// VGroup Operations
	PRIV_VG_BALANCE        = 50 // BALANCE VGROUP
	PRIV_VG_BALANCE_LEADER = 51 // BALANCE VGROUP LEADER
	PRIV_VG_MERGE          = 52 // MERGE VGROUP
	PRIV_VG_REDISTRIBUTE   = 53 // REDISTRIBUTE VGROUP
	PRIV_VG_SPLIT          = 54 // SPLIT VGROUP

	// DB Show Operations
	PRIV_SHOW_VNODES     = 60 // SHOW VNODES
	PRIV_SHOW_VGROUPS    = 61 // SHOW VGROUPS
	PRIV_SHOW_COMPACTS   = 62 // SHOW COMPACTS
	PRIV_SHOW_RETENTIONS = 63 // SHOW RETENTIONS
	PRIV_SHOW_SCANS      = 64 // SHOW SCANS
	PRIV_SHOW_SSMIGRATES = 65 // SHOW SSMIGRATES

	// ==================== Table Privileges(50-69)  ================
	PRIV_TBL_CREATE = 70 // CREATE TABLE
	PRIV_TBL_SELECT = 71 // SELECT TABLE
	PRIV_TBL_INSERT = 72 // INSERT TABLE
	PRIV_TBL_UPDATE = 73 // UPDATE TABLE(reserved)
	PRIV_TBL_DELETE = 74 // DELETE TABLE

	// ==================== Other Privileges ================
	// function management
	PRIV_FUNC_CREATE = 80 // CREATE FUNCTION
	PRIV_FUNC_DROP   = 81 // DROP FUNCTION
	PRIV_FUNC_SHOW   = 82 // SHOW FUNCTIONS

	// index management
	PRIV_IDX_CREATE = 84 // CREATE INDEX
	PRIV_IDX_DROP   = 85 //  DROP INDEX
	PRIV_IDX_SHOW   = 86 //  SHOW INDEXES

	// view management
	PRIV_VIEW_CREATE = 88 // CREATE VIEW
	PRIV_VIEW_SELECT = 89 // SELECT VIEW

	// SMA management
	PRIV_RSMA_CREATE = 95 // CREATE RSMA
	PRIV_TSMA_CREATE = 96 // CREATE TSMA

	// mount management
	PRIV_MOUNT_CREATE = 100 // CREATE MOUNT
	PRIV_MOUNT_DROP   = 101 // DROP MOUNT
	PRIV_MOUNT_SHOW   = 102 // SHOW MOUNTS

	// password management
	PRIV_PASS_ALTER      = 105 // ALTER PASS
	PRIV_PASS_ALTER_SELF = 106 // ALTER SELF PASS

	// role management
	PRIV_ROLE_CREATE = 110 // CREATE ROLE
	PRIV_ROLE_DROP   = 111 // DROP ROLE
	PRIV_ROLE_SHOW   = 112 // SHOW ROLES
	PRIV_ROLE_LOCK   = 113 // LOCK ROLE
	PRIV_ROLE_UNLOCK = 114 // UNLOCK ROLE

	// user management
	PRIV_USER_CREATE       = 130 // CREATE USER
	PRIV_USER_DROP         = 131 // DROP USER
	PRIV_USER_SET_SECURITY = 132 // SET USER SECURITY INFO
	PRIV_USER_SET_AUDIT    = 133 // SET USER AUDIT INFO
	PRIV_USER_SET_BASIC    = 134 // SET USER BASIC INFO
	PRIV_USER_UNLOCK       = 135 // UNLOCK USER
	PRIV_USER_LOCK         = 136 // LOCK USER
	PRIV_USER_SHOW         = 137 // SHOW USERS

	// audit management
	PRIV_AUDIT_DB_CREATE = 140 // CREATE AUDIT DATABASE
	PRIV_AUDIT_DB_DROP   = 141 // DROP AUDIT DATABASE
	PRIV_AUDIT_DB_ALTER  = 142 // ALTER AUDIT DATABASE
	PRIV_AUDIT_DB_USE    = 143 // USE AUDIT DATABASE

	// token management
	PRIV_TOKEN_CREATE = 150 // CREATE TOKEN
	PRIV_TOKEN_DROP   = 151 // DROP TOKEN
	PRIV_TOKEN_ALTER  = 152 // ALTER TOKEN
	PRIV_TOKEN_SHOW   = 153 // SHOW TOKENS

	// key management
	PRIV_KEY_UPDATE  = 160 // UPDATE KEY
	PRIV_TOTP_CREATE = 161 // CREATE TOTP
	PRIV_TOTP_DROP   = 162 // DROP TOTP
	PRIV_TOTP_UPDATE = 163 // UPDATE TOTP

	// grant/revoke privileges
	PRIV_GRANT_PRIVILEGE  = 170 // GRANT PRIVILEGE
	PRIV_REVOKE_PRIVILEGE = 171 // REVOKE PRIVILEGE
	PRIV_SHOW_PRIVILEGES  = 172 // SHOW PRIVILEGES
	PRIV_GRANT_SYSDBA     = 173 // GRANT SYSDBA PRIVILEGE
	PRIV_REVOKE_SYSDBA    = 174 // REVOKE SYSDBA PRIVILEGE
	PRIV_GRANT_SYSSEC     = 175 // GRANT SYSSEC PRIVILEGE
	PRIV_REVOKE_SYSSEC    = 176 // REVOKE SYSSEC PRIVILEGE
	PRIV_GRANT_SYSAUDIT   = 177 // GRANT SYSAUDIT PRIVILEGE
	PRIV_REVOKE_SYSAUDIT  = 178 // REVOKE SYSAUDIT PRIVILEGE

	// node management
	PRIV_NODE_CREATE = 190 // CREATE NODE
	PRIV_NODE_DROP   = 191 // DROP NODE
	PRIV_NODES_SHOW  = 192 // SHOW NODES

	// system variables
	PRIV_VAR_SECURITY_ALTER = 200 // ALTER SECURITY VARIABLE
	PRIV_VAR_AUDIT_ALTER    = 201 // ALTER AUDIT VARIABLE
	PRIV_VAR_SYSTEM_ALTER   = 202 // ALTER SYSTEM VARIABLE
	PRIV_VAR_DEBUG_ALTER    = 203 // ALTER DEBUG VARIABLE
	PRIV_VAR_SECURITY_SHOW  = 204 // SHOW SECURITY VARIABLES
	PRIV_VAR_AUDIT_SHOW     = 205 // SHOW AUDIT VARIABLES
	PRIV_VAR_SYSTEM_SHOW    = 206 // SHOW SYSTEM VARIABLES
	PRIV_VAR_DEBUG_SHOW     = 207 // SHOW DEBUG VARIABLES

	// topic management
	PRIV_TOPIC_CREATE      = 210 // CREATE TOPIC
	PRIV_CONSUMER_SHOW     = 211 // SHOW CONSUMERS
	PRIV_SUBSCRIPTION_SHOW = 212 // SHOW SUBSCRIPTIONS

	// stream management
	PRIV_STREAM_CREATE = 220 // CREATE STREAM

	// system operation management
	PRIV_TRANS_SHOW      = 230 // SHOW TRANS
	PRIV_TRANS_KILL      = 231 // KILL TRANS
	PRIV_CONNECTION_SHOW = 232 // SHOW CONNECTIONS
	PRIV_CONNECTION_KILL = 233 // KILL CONNECTION
	PRIV_QUERY_SHOW      = 234 // SHOW QUERIES
	PRIV_QUERY_KILL      = 235 // KILL QUERY

	// system info
	PRIV_INFO_SCHEMA_USE        = 240 // USE INFORMATION_SCHEMA
	PRIV_PERF_SCHEMA_USE        = 241 // USE PERFORMANCE_SCHEMA
	PRIV_INFO_SCHEMA_READ_LIMIT = 242 // READ INFORMATION_SCHEMA LIMIT
	PRIV_INFO_SCHEMA_READ_SEC   = 243 // READ INFORMATION_SCHEMA SECURITY
	PRIV_INFO_SCHEMA_READ_AUDIT = 244 // READ INFORMATION_SCHEMA AUDIT
	PRIV_INFO_SCHEMA_READ_BASIC = 245 // READ INFORMATION_SCHEMA BASIC
	PRIV_PERF_SCHEMA_READ_LIMIT = 246 // READ PERFORMANCE_SCHEMA LIMIT
	PRIV_PERF_SCHEMA_READ_BASIC = 247 // READ PERFORMANCE_SCHEMA BASIC
	PRIV_GRANTS_SHOW            = 248 // SHOW GRANTS
	PRIV_CLUSTER_SHOW           = 249 // SHOW CLUSTER
	PRIV_APPS_SHOW              = 250 // SHOW APPS

	// extended privileges can be defined here (255 bits reserved in total)
	// ==================== Maximum Privilege Bit ====================
	MAX_PRIV_TYPE = 255
)
const (
	PRIV_GROUP_CNT = ((MAX_PRIV_TYPE + 63) / 64)
)

type PrivSet struct {
	Set [PRIV_GROUP_CNT]uint64
}

type PrivSetArgs struct {
	PrivSet    PrivSet
	PrivArgs   int32
	ObjType    int16 // EPrivObjType
	selectCols []string
	insertCols []string
	updateCols []string
}

const (
	PRIV_OBJ_CLUSTER int16 = iota
	PRIV_OBJ_DB
	PRIV_OBJ_TBL
)

type GrantStmt struct {
	OptrType      int8
	Principal     string
	ObjName       string
	TabName       string
	RoleName      string
	PrivilegeName string
	Privileges    PrivSetArgs
	Cond          interface{}
}

func (s *GrantStmt) iStatement() {}

func (s *GrantStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	if s.OptrType == 0 {
		buf.Myprintf("grant")
	} else {
		buf.Myprintf("revoke")
	}
	if s.PrivilegeName != "" {
		buf.Myprintf(" %s", s.PrivilegeName)
	} else if s.Privileges.PrivArgs == PRIV_CM_ALL {
		buf.Myprintf(" all")
	} else {
		buf.Myprintf(" read")
	}
	switch s.Privileges.ObjType {
	case PRIV_OBJ_DB:
		if s.ObjName != "" {
			buf.Myprintf(" on database %s", s.ObjName)
		}
	case PRIV_OBJ_TBL:
		if s.TabName != "" && s.ObjName != "" {
			buf.Myprintf(" on table %s.%s", s.ObjName, s.TabName)
		} else if s.ObjName != "" {
			buf.Myprintf(" on table %s", s.ObjName)
		} else if s.TabName != "" {
			buf.Myprintf(" on table %s", s.TabName)
		}
	}
	if condExpr, ok := s.Cond.(Expr); ok && condExpr != nil {
		buf.Myprintf(" with %v", condExpr)
	}
	if s.Principal != "" {
		if s.OptrType == 0 {
			buf.Myprintf(" to %s", s.Principal)
		} else {
			buf.Myprintf(" from %s", s.Principal)
		}
	}
}

func (s *GrantStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	if condExpr, ok := s.Cond.(Expr); ok && condExpr != nil {
		if err := Walk(visit, condExpr); err != nil {
			return err
		}
	}
	return nil
}
