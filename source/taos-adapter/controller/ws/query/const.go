package query

const TaosSessionKey = "taos"
const (
	WSConnect                 = "conn"
	WSQuery                   = "query"
	WSFetch                   = "fetch"
	WSFetchBlock              = "fetch_block"
	WSFreeResult              = "free_result"
	WSWriteRaw                = "write_raw"
	WSWriteRawBlock           = "write_raw_block"
	WSWriteRawBlockWithFields = "write_raw_block_with_fields"
)
const (
	TMQRawMessage             = 3
	RawBlockMessage           = 4
	RawBlockMessageWithFields = 5
)
