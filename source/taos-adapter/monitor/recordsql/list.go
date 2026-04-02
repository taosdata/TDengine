package recordsql

import (
	"container/list"
	"sync"
)

type RecordList struct {
	list *list.List
	lock sync.Mutex
}

func NewRecordList() *RecordList {
	return &RecordList{
		list: list.New(),
	}
}

// Add adds a new item to the end of the list and returns the element.
func (rl *RecordList) Add(item interface{}) *list.Element {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	return rl.list.PushBack(item)
}

// Remove removes the element from the recordList and returns the value stored in it.
// If return nil, it means the element is already removed.
func (rl *RecordList) Remove(ele *list.Element) interface{} {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	if ele != nil {
		val := rl.list.Remove(ele)
		ele.Value = nil
		return val
	}
	return nil
}

// RemoveAllSqlRecords removes all elements from the recordList and returns a slice of the removed records.
// Each element in the recordList is removed and its value is set to nil,
// in this way RecordList.Remove can check if the element is removed already.
func (rl *RecordList) RemoveAllSqlRecords() []*SQLRecord {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	count := rl.list.Len()
	records := make([]*SQLRecord, count)
	for i := 0; i < count; i++ {
		ele := rl.list.Front()
		records[i] = rl.list.Remove(ele).(*SQLRecord)
		if ele != nil {
			ele.Value = nil
		}
	}
	return records
}

func (rl *RecordList) RemoveAllStmtRecords() []*StmtRecord {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	count := rl.list.Len()
	records := make([]*StmtRecord, count)
	for i := 0; i < count; i++ {
		ele := rl.list.Front()
		records[i] = rl.list.Remove(ele).(*StmtRecord)
		if ele != nil {
			ele.Value = nil
		}
	}
	return records
}
