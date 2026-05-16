package tmq

import "fmt"

type Meta struct {
	Type          string        `json:"type"`
	TableName     string        `json:"tableName"`
	TableType     string        `json:"tableType"`
	CreateList    []*CreateItem `json:"createList"`
	Columns       []*Column     `json:"columns"`
	Using         string        `json:"using"`
	TagNum        int           `json:"tagNum"`
	Tags          []*Tag        `json:"tags"`
	TableNameList []string      `json:"tableNameList"`
	AlterType     int           `json:"alterType"`
	ColName       string        `json:"colName"`
	ColNewName    string        `json:"colNewName"`
	ColType       int           `json:"colType"`
	ColLength     int           `json:"colLength"`
	ColValue      string        `json:"colValue"`
	ColValueNull  bool          `json:"colValueNull"`
}

type Tag struct {
	Name  string      `json:"name"`
	Type  int         `json:"type"`
	Value interface{} `json:"value"`
}

type Column struct {
	Name   string `json:"name"`
	Type   int    `json:"type"`
	Length int    `json:"length"`
}

type CreateItem struct {
	TableName string `json:"tableName"`
	Using     string `json:"using"`
	TagNum    int    `json:"tagNum"`
	Tags      []*Tag `json:"tags"`
}

type Offset int64

const OffsetInvalid = Offset(-2147467247)

func (o Offset) String() string {
	if o == OffsetInvalid {
		return "unset"
	}
	return fmt.Sprintf("%d", int64(o))
}

func (o Offset) Valid() bool {
	if o < 0 && o != OffsetInvalid {
		return false
	}
	return true
}

type TopicPartition struct {
	Topic     *string
	Partition int32
	Offset    Offset
	Metadata  *string
	Error     error
}

func (p TopicPartition) String() string {
	topic := "<null>"
	if p.Topic != nil {
		topic = *p.Topic
	}
	if p.Error != nil {
		return fmt.Sprintf("%s[%d]@%s(%s)",
			topic, p.Partition, p.Offset, p.Error)
	}
	return fmt.Sprintf("%s[%d]@%s",
		topic, p.Partition, p.Offset)
}

type Assignment struct {
	VGroupID int32 `json:"vgroup_id"`
	Offset   int64 `json:"offset"`
	Begin    int64 `json:"begin"`
	End      int64 `json:"end"`
}
