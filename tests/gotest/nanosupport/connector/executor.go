package connector

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/taosdata/go-utils/log"
	"github.com/taosdata/go-utils/tdengine/config"
	"github.com/taosdata/go-utils/tdengine/connector"
	tdengineExecutor "github.com/taosdata/go-utils/tdengine/executor"
)

type Executor struct {
	executor *tdengineExecutor.Executor
	ctx      context.Context
}

var Logger = log.NewLogger("taos test")

func NewExecutor(conf *config.TDengineGo, db string, showSql bool) (*Executor, error) {
	tdengineConnector, err := connector.NewTDengineConnector("go", conf)
	if err != nil {
		return nil, err
	}
	executor := tdengineExecutor.NewExecutor(tdengineConnector, db, showSql, Logger)
	return &Executor{
		executor: executor,
		ctx:      context.Background(),
	}, nil
}

func (e *Executor) Execute(sql string) (int64, error) {
	return e.executor.DoExec(e.ctx, sql)
}
func (e *Executor) Query(sql string) (*connector.Data, error) {
	fmt.Println("query :", sql)
	return e.executor.DoQuery(e.ctx, sql)
}
func (e *Executor) CheckData(row, col int, value interface{}, data *connector.Data) (bool, error) {
	if data == nil {
		return false, fmt.Errorf("data is nil")
	}
	if col >= len(data.Head) {
		return false, fmt.Errorf("col out of data")
	}
	if row >= len(data.Data) {
		return false, fmt.Errorf("row out of data")
	}
	dataValue := data.Data[row][col]

	if dataValue == nil && value != nil {
		return false, fmt.Errorf("dataValue is nil but value is not nil")
	}
	if dataValue == nil && value == nil {
		return true, nil
	}
	if reflect.TypeOf(dataValue) != reflect.TypeOf(value) {
		return false, fmt.Errorf("type not match expect %s got %s", reflect.TypeOf(value), reflect.TypeOf(dataValue))
	}
	switch value.(type) {
	case time.Time:
		t, _ := dataValue.(time.Time)
		if value.(time.Time).Nanosecond() != t.Nanosecond() {
			return false, fmt.Errorf("value not match expect %d got %d", value.(time.Time).Nanosecond(), t.Nanosecond())
		}
	case string:
		if value.(string) != dataValue.(string) {
			return false, fmt.Errorf("value not match expect %s got %s", value.(string), dataValue.(string))
		}
	case int8:
		if value.(int8) != dataValue.(int8) {
			return false, fmt.Errorf("value not match expect %d got %d", value.(int8), dataValue.(int8))
		}
	case int16:
		if value.(int16) != dataValue.(int16) {
			return false, fmt.Errorf("value not match expect %d got %d", value.(int16), dataValue.(int16))
		}
	case int32:
		if value.(int32) != dataValue.(int32) {
			return false, fmt.Errorf("value not match expect %d got %d", value.(int32), dataValue.(int32))
		}
	case int64:
		if value.(int64) != dataValue.(int64) {
			return false, fmt.Errorf("value not match expect %d got %d", value.(int64), dataValue.(int64))
		}
	case float32:
		if value.(float32) != dataValue.(float32) {
			return false, fmt.Errorf("value not match expect %f got %f", value.(float32), dataValue.(float32))
		}
	case float64:
		if value.(float64) != dataValue.(float64) {
			return false, fmt.Errorf("value not match expect %f got %f", value.(float32), dataValue.(float32))
		}
	case bool:
		if value.(bool) != dataValue.(bool) {
			return false, fmt.Errorf("value not match expect %t got %t", value.(bool), dataValue.(bool))
		}
	default:
		return false, fmt.Errorf("unsupport type %v", reflect.TypeOf(value))
	}
	return true, nil
}

func (e *Executor) CheckData2(row, col int, value interface{}, data *connector.Data) {

	match, err := e.CheckData(row, col, value, data)
	fmt.Println("expect data is :", value)
	fmt.Println("go got data is :", data.Data[row][col])
	if err != nil {
		fmt.Println(err)
	}
	if !match {
		fmt.Println(" data not match")

	}

	/*
		fmt.Println(value)
		if data == nil {
			// return false, fmt.Errorf("data is nil")
			// fmt.Println("check failed")
		}
		if col >= len(data.Head) {
			// return false, fmt.Errorf("col out of data")
			// fmt.Println("check failed")
		}
		if row >= len(data.Data) {
			// return false, fmt.Errorf("row out of data")
			// fmt.Println("check failed")
		}
		dataValue := data.Data[row][col]

		if dataValue == nil && value != nil {
			// return false, fmt.Errorf("dataValue is nil but value is not nil")
			// fmt.Println("check failed")
		}
		if dataValue == nil && value == nil {
			// return true, nil
			fmt.Println("check pass")
		}
		if reflect.TypeOf(dataValue) != reflect.TypeOf(value) {
			// return false, fmt.Errorf("type not match expect %s got %s", reflect.TypeOf(value), reflect.TypeOf(dataValue))
			fmt.Println("check failed")
		}
		switch value.(type) {
		case time.Time:
			t, _ := dataValue.(time.Time)
			if value.(time.Time).Nanosecond() != t.Nanosecond() {
				// return false, fmt.Errorf("value not match expect %d got %d", value.(time.Time).Nanosecond(), t.Nanosecond())
				// fmt.Println("check failed")
			}
		case string:
			if value.(string) != dataValue.(string) {
				// return false, fmt.Errorf("value not match expect %s got %s", value.(string), dataValue.(string))
				// fmt.Println("check failed")
			}
		case int8:
			if value.(int8) != dataValue.(int8) {
				// return false, fmt.Errorf("value not match expect %d got %d", value.(int8), dataValue.(int8))
				// fmt.Println("check failed")
			}
		case int16:
			if value.(int16) != dataValue.(int16) {
				// return false, fmt.Errorf("value not match expect %d got %d", value.(int16), dataValue.(int16))
				// fmt.Println("check failed")
			}
		case int32:
			if value.(int32) != dataValue.(int32) {
				// return false, fmt.Errorf("value not match expect %d got %d", value.(int32), dataValue.(int32))
				// fmt.Println("check failed")
			}
		case int64:
			if value.(int64) != dataValue.(int64) {
				// return false, fmt.Errorf("value not match expect %d got %d", value.(int64), dataValue.(int64))
				// fmt.Println("check failed")
			}
		case float32:
			if value.(float32) != dataValue.(float32) {
				// return false, fmt.Errorf("value not match expect %f got %f", value.(float32), dataValue.(float32))
				// fmt.Println("check failed")
			}
		case float64:
			if value.(float64) != dataValue.(float64) {
				// return false, fmt.Errorf("value not match expect %f got %f", value.(float32), dataValue.(float32))
				// fmt.Println("check failed")
			}
		case bool:
			if value.(bool) != dataValue.(bool) {
				// return false, fmt.Errorf("value not match expect %t got %t", value.(bool), dataValue.(bool))
				// fmt.Println("check failed")
			}
		default:
			// return false, fmt.Errorf("unsupport type %v", reflect.TypeOf(value))
			// fmt.Println("check failed")
		}
		// return true, nil
		// fmt.Println("check pass")
	*/
}

func (e *Executor) CheckRow(count int, data *connector.Data) {

	if len(data.Data) != count {
		fmt.Println("check failed !")
	}
}
