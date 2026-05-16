package recordsql

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRecordList(t *testing.T) {
	rl := NewRecordList()
	assert.NotNil(t, rl)
	assert.NotNil(t, rl.list)
	assert.Equal(t, 0, rl.list.Len())
}

func TestRecordList_Add(t *testing.T) {
	t.Run("Single Add", func(t *testing.T) {
		rl := NewRecordList()
		record := &SQLRecord{SQL: "SELECT 1"}
		ele := rl.Add(record)

		assert.NotNil(t, ele)
		assert.Equal(t, record, ele.Value.(*SQLRecord))
		assert.Equal(t, 1, rl.list.Len())
	})

	t.Run("Concurrent Adds", func(t *testing.T) {
		rl := NewRecordList()
		var wg sync.WaitGroup
		count := 100

		wg.Add(count)
		for i := 0; i < count; i++ {
			go func(i int) {
				defer wg.Done()
				record := &SQLRecord{SQL: "SELECT " + string(rune(i))}
				rl.Add(record)
			}(i)
		}
		wg.Wait()

		assert.Equal(t, count, rl.list.Len())
	})
}

func TestRecordList_Remove(t *testing.T) {
	t.Run("Remove Existing Element", func(t *testing.T) {
		rl := NewRecordList()
		record := &SQLRecord{SQL: "SELECT 1"}
		ele := rl.Add(record)

		removed := rl.Remove(ele)
		assert.Equal(t, record, removed)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Remove Nil Element", func(t *testing.T) {
		rl := NewRecordList()
		record := &SQLRecord{SQL: "SELECT 1"}
		rl.Add(record)

		removed := rl.Remove(nil)
		assert.Nil(t, removed)
		assert.Equal(t, 1, rl.list.Len())
	})

	t.Run("Remove Already Removed Element", func(t *testing.T) {
		rl := NewRecordList()
		record := &SQLRecord{SQL: "SELECT 1"}
		ele := rl.Add(record)
		rl.Remove(ele)

		removed := rl.Remove(ele)
		assert.Nil(t, removed)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Concurrent Remove", func(t *testing.T) {
		rl := NewRecordList()
		record := &SQLRecord{SQL: "SELECT 1"}
		ele := rl.Add(record)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			rl.Remove(ele)
		}()
		go func() {
			defer wg.Done()
			rl.Remove(ele)
		}()
		wg.Wait()

		assert.Equal(t, 0, rl.list.Len())
	})
}

func TestRecordList_RemoveAllSqlRecords(t *testing.T) {
	t.Run("Empty List", func(t *testing.T) {
		rl := NewRecordList()
		records := rl.RemoveAllSqlRecords()
		assert.Empty(t, records)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Single Item", func(t *testing.T) {
		rl := NewRecordList()
		record := &SQLRecord{SQL: "SELECT 1"}
		rl.Add(record)

		records := rl.RemoveAllSqlRecords()
		assert.Equal(t, 1, len(records))
		assert.Equal(t, record, records[0])
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Multiple Items", func(t *testing.T) {
		rl := NewRecordList()
		count := 10
		expectedRecords := make([]*SQLRecord, count)

		for i := 0; i < count; i++ {
			record := &SQLRecord{SQL: "SELECT " + string(rune(i))}
			expectedRecords[i] = record
			rl.Add(record)
		}

		records := rl.RemoveAllSqlRecords()
		assert.Equal(t, count, len(records))
		assert.ElementsMatch(t, expectedRecords, records)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Concurrent RemoveAllSqlRecords", func(t *testing.T) {
		rl := NewRecordList()
		count := 100
		for i := 0; i < count; i++ {
			rl.Add(&SQLRecord{SQL: "SELECT " + string(rune(i))})
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var records1, records2 []*SQLRecord
		go func() {
			defer wg.Done()
			records1 = rl.RemoveAllSqlRecords()
		}()
		go func() {
			defer wg.Done()
			records2 = rl.RemoveAllSqlRecords()
		}()
		wg.Wait()

		// Only one RemoveAllSqlRecords should get all records
		assert.True(t, (len(records1) == count && len(records2) == 0) ||
			(len(records1) == 0 && len(records2) == count))
		assert.Equal(t, 0, rl.list.Len())
	})
}

func TestRecordList_RemoveAllStmtRecords(t *testing.T) {
	t.Run("Empty List", func(t *testing.T) {
		rl := NewRecordList()
		records := rl.RemoveAllStmtRecords()
		assert.Empty(t, records)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Single Item", func(t *testing.T) {
		rl := NewRecordList()
		record := &StmtRecord{SQL: "SELECT 1"}
		rl.Add(record)

		records := rl.RemoveAllStmtRecords()
		assert.Equal(t, 1, len(records))
		assert.Equal(t, record, records[0])
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Multiple Items", func(t *testing.T) {
		rl := NewRecordList()
		count := 10
		expectedRecords := make([]*StmtRecord, count)

		for i := 0; i < count; i++ {
			record := &StmtRecord{SQL: "SELECT " + string(rune(i))}
			expectedRecords[i] = record
			rl.Add(record)
		}

		records := rl.RemoveAllStmtRecords()
		assert.Equal(t, count, len(records))
		assert.ElementsMatch(t, expectedRecords, records)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Concurrent RemoveAllStmtRecords", func(t *testing.T) {
		rl := NewRecordList()
		count := 100
		for i := 0; i < count; i++ {
			rl.Add(&StmtRecord{SQL: "SELECT " + string(rune(i))})
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var records1, records2 []*StmtRecord
		go func() {
			defer wg.Done()
			records1 = rl.RemoveAllStmtRecords()
		}()
		go func() {
			defer wg.Done()
			records2 = rl.RemoveAllStmtRecords()
		}()
		wg.Wait()

		// Only one RemoveAllStmtRecords should get all records
		assert.True(t, (len(records1) == count && len(records2) == 0) ||
			(len(records1) == 0 && len(records2) == count))
		assert.Equal(t, 0, rl.list.Len())
	})
}
