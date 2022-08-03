package zorm_example_td

import (
	"context"
	"fmt"
	"gitee.com/chunanyong/zorm"
	_ "github.com/taosdata/driver-go/v2/taosSql"
	"testing"
	"time"
)

/**
 *	@ Test environment
 *	  TDengine Version：2.4.0.7
 *    Drive Version: github.com/taosdata/driver-go/v2 v2.0.4
 *	  ZORM Version: 1.5.8
 *    use go module
 *
 *	@ Illustrate：
 *	  因为TDengine的驱动在对占位符拼接的时候不会对字符串进行单引号拼接。
 *	  提了issue,官方解答说 "是为兼容之前设计，需要手动加上单引号"
 *	  所以我们把Sql语句进行处理,
 *	  issues地址:https://github.com/taosdata/TDengine/issues/15393
 *
 *	@ Official website: www.zorm.cn(logo暂时很丑 希望以后有更多人加入可以帮帮我们)
 *	@ Project address:  https://gitee.com/chunanyong/zorm
 *
 */

//SuperTable nme and Database name.
//库名与超级表名称
const (
	DBName           = "ZORM"
	SuperTableBinary = "SuperTableBinary"
	SuperTableDouble = "SuperTableDouble"
	SuperTableBigInt = "SuperTableBigInt"
	SuperTableBool   = "SuperTableBool"
)

type Demo struct {
	zorm.EntityStruct
	Time      time.Time `column:"time"`
	Value     float64   `column:"value"`
	TableName string
}

type TestBinary struct {
	zorm.EntityStruct
	Time      time.Time `column:"time"`
	Value     string    `column:"value"`
	TableName string
}

func (entity *Demo) GetTableName() string {
	return entity.TableName
}

func (entity *Demo) GetPKColumnName() string {
	return ""
}
func (entity *TestBinary) GetTableName() string {
	return entity.TableName
}
func (entity *TestBinary) GetPKColumnName() string {
	return ""
}

var dbDao *zorm.DBDao
var ctx = context.Background()

// Initialize connection
// 初始化连接
// 因为taos的Rest驱动在实际使用中 出现了无法跨IP访问的异常 所以不推荐使用 但是也可以使用ZORM进行操作 只需要替换DSN与DriverName
// 本次只展示配合客服端的官方驱动包
func init() {
	dbDaoConfig := zorm.DataSourceConfig{
		//DSN 数据库的连接字符串
		DSN: "root:taosdata@tcp(172.16.66.31:6030)/",
		//数据库驱动名称:mysql,postgres,oci8,sqlserver,sqlite3,clickhouse,dm,kingbase,aci 和DBType对应,处理数据库有多个驱动
		DriverName: "taosSql",
		//数据库类型(方言判断依据):mysql,postgresql,oracle,mssql,sqlite,clickhouse,dm,kingbase,shentong 和 DriverName 对应,处理数据库有多个驱动
		DBType: "tdengine",
		//MaxOpenConns 数据库最大连接数 默认50
		MaxOpenConns: 10,
		//MaxIdleConns 数据库最大空闲连接数 默认50
		MaxIdleConns: 10,
		//ConnMaxLifetimeSecond 连接存活秒时间. 默认600(10分钟)后连接被销毁重建.避免数据库主动断开连接,造成死连接.MySQL默认wait_timeout 28800秒(8小时)
		ConnMaxLifetimeSecond: 600,
		//PrintSQL 打印SQL.会使用FuncPrintSQL记录SQL
		PrintSQL:           true,
		DisableTransaction: true, // 禁用全局事务
	}
	dbDao, _ = zorm.NewDBDao(&dbDaoConfig)

}

// Test_Create Create DB and table.
// Test_Create 建库建st表
func Test_create_st(t *testing.T) {

	defer dbDao.CloseDB()
	finder := zorm.NewFinder()
	finder.Append("create database if not exists ? precision ?  keep ? ", DBName, "us", 365)
	_, err := zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to create database, err: %v", err)
	}

	finder = zorm.NewFinder()
	// 艰难取舍
	// 因为会对string进行加单引号 所以在创建数据类型的时候请不要使用占位符 (毕竟创建超级表的地方不算多)
	finder.Append("create table if not exists  ?.? (Time TIMESTAMP, Value Bool) TAGS (model_id BINARY(64), model_name BINARY(64))", DBName, SuperTableBool)
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to create table, err: %v", err)
	}

	//如果使用循环创建方式可以这样操作
	tables := make(map[string]string, 3)
	tables[SuperTableBinary] = "BINARY(64)"
	tables[SuperTableBigInt] = "BIGINT"
	tables[SuperTableDouble] = "Double"
	for tableName, tableType := range tables {
		sql := fmt.Sprintf("create table if not exists  %s.%s (Time TIMESTAMP, Value %s) TAGS (model_id BINARY(64), model_name BINARY(64))", DBName, tableName, tableType)
		finder = zorm.NewFinder()
		finder.Append(sql)
		_, err = zorm.UpdateFinder(ctx, finder)
		if err != nil {
			t.Errorf("failed to create table, err: %v", err)
		}
	}
	//或者
	//tables := make(map[string]string, 3)
	//tables[SuperTableBinary] = "BINARY(64)"
	//tables[SuperTableBigInt] = "BIGINT"
	//tables[SuperTableDouble] = "Double"
	//for tableName, tableType := range tables {
	//	sql := fmt.Sprintf("create table if not exists  ?.? (Time TIMESTAMP, Value %s) TAGS (model_id BINARY(64), model_name BINARY(64))", tableType)
	//	finder = zorm.NewFinder()
	//	finder.Append(sql, DBName, tableName)
	//	_, err = zorm.UpdateFinder(ctx, finder)
	//	if err != nil {
	//		t.Errorf("failed to create table, err: %v", err)
	//	}
	//}
}

//创建子表
//Create tables
func Test_create_tables(t *testing.T) {

	defer dbDao.CloseDB()
	finder := zorm.NewFinder()
	finder.Append(`create table if not exists ?.? using ?.? TAGS(?,?) `, DBName, "zorm001", DBName, SuperTableDouble, "001", "zorm")
	_, err := zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to create SuperTableBigint, err: %v", err)
	}

	finder = zorm.NewFinder()
	finder.Append(`create table if not exists ?.? using ?.? TAGS(?,?) `, DBName, "zorm002", DBName, SuperTableBool, "002", "zorm")
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to create SuperTableDouble, err: %v", err)
	}
	finder = zorm.NewFinder()
	finder.Append(`create table if not exists ?.? using ?.? TAGS(?,?) `, DBName, "zorm003", DBName, SuperTableBinary, "003", "zorm")
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to create SuperTableBinary, err: %v", err)
	}

	finder = zorm.NewFinder()
	finder.Append(`create table if not exists ?.? using ?.? TAGS(?,?) `, DBName, "zorm004", DBName, SuperTableBinary, "004", "zorm")
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to create SuperTableBool, err: %v", err)
	}

	finder = zorm.NewFinder()
	finder.Append(`create table if not exists ?.? using ?.? TAGS(?,?) `, DBName, "zorm005", DBName, SuperTableBinary, "005", "zorm")
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to create SuperTableBinary, err: %v", err)
	}
	finder = zorm.NewFinder()
	finder.Append(`create table if not exists ?.? using ?.? TAGS(?,?) `, DBName, "zorm006", DBName, SuperTableDouble, "006", "zorm")
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to create SuperTableBinary, err: %v", err)
	}
	finder = zorm.NewFinder()
	finder.Append(`create table if not exists ?.? using ?.? TAGS(?,?) `, DBName, "zorm007", DBName, SuperTableBigInt, "007", "zorm")
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to create SuperTableBinary, err: %v", err)
	}
}

//结构体插入单条
func Test_inset_struct(t *testing.T) {
	defer dbDao.CloseDB()
	//因为td选择库会丢失 （实际使用中会报未选择库）
	//在实际应用中可能会有跨库操作的需求 因此需要自己添加上库名前缀
	//库名.表名
	demo := Demo{Time: time.Now(), Value: 927, TableName: fmt.Sprintf("%s.%s", DBName, "zorm001")}
	num, err := zorm.Insert(ctx, &demo)
	if err != nil {
		t.Errorf("%v", err)
	}
	t.Logf("插入%d条", num)
}

//Test_bulk_inset_struct 结构体插入单条
func Test_bulk_inset_struct(t *testing.T) {
	/**
	插入数据时请确保使用的时候同一结构体
	不支持 insert into tableName1 values(v1,v2)  tableName2 values(v1,v2,v3)
	请确保结构体属性与表字段 顺序一致很重要
	考虑到插入长度限制的效率问题 我们将sql 拼接成了
	insert into tableName1 values(?,?)  tableName2 values(?,?)
	并没有拼成  insert into tableName (column1,column2) values (?,?)
	这样能保证批量插入的效率
	所以请确保 结构体属性与表字段的顺序一致
	*/
	defer dbDao.CloseDB()
	var demos = make([]zorm.IEntityStruct, 0)
	//相同结构的的子表（同一超级表下子表,如果不是必须保证类型一致）
	//tableName 是可以替换的   demo定义的是超级表结构
	demo1 := Demo{Time: time.Now(), Value: 188, TableName: fmt.Sprintf("%s.%s", DBName, "zorm001")}
	demo2 := Demo{Time: time.Now(), Value: 189, TableName: fmt.Sprintf("%s.%s", DBName, "zorm006")}
	demo3 := Demo{Time: time.Now(), Value: 190, TableName: fmt.Sprintf("%s.%s", DBName, "zorm001")}
	demo4 := Demo{Time: time.Now(), Value: 191, TableName: fmt.Sprintf("%s.%s", DBName, "zorm001")}
	demo5 := Demo{Time: time.Now(), Value: 192, TableName: fmt.Sprintf("%s.%s", DBName, "zorm001")}
	demo6 := Demo{Time: time.Now(), Value: 193, TableName: fmt.Sprintf("%s.%s", DBName, "zorm001")}
	demo7 := Demo{Time: time.Now(), Value: 193, TableName: fmt.Sprintf("%s.%s", DBName, "zorm007")}
	demos = append(demos, &demo1, &demo2, &demo3, &demo4, &demo5, &demo6, &demo7) //注意需要指针传递
	num, err := zorm.InsertSlice(ctx, demos)
	if err != nil {
		t.Errorf("%v", err)
	}
	t.Logf("插入%d条", num)
}

//Test_insert_map 插入单个map
func Test_insert_map(t *testing.T) {
	defer dbDao.CloseDB()
	entityMap := zorm.NewEntityMap(fmt.Sprintf("%s.%s", DBName, "zorm005"))
	entityMap.PkColumnName = "" //不写此行也可以 但是会打印log
	entityMap.Set("time", time.Now())
	entityMap.Set("value", "hello")
	num, err := zorm.InsertEntityMap(ctx, entityMap)
	if err != nil {
		t.Errorf("%v", err)
	}
	t.Logf("插入%d条", num)

	entityMap = zorm.NewEntityMap(fmt.Sprintf("%s.%s", DBName, "zorm001"))
	entityMap.PkColumnName = ""
	entityMap.Set("time", time.Now())
	entityMap.Set("value", 365)
	num, err = zorm.InsertEntityMap(ctx, entityMap)
	if err != nil {
		t.Errorf("%v", err)
	}
	t.Logf("插入%d条", num)
}

//Test_use_updateFinder 使用updateFinder 进行插入
func Test_use_updateFinder(t *testing.T) {
	defer dbDao.CloseDB()
	finder := zorm.NewFinder()
	finder.Append("insert into ?.?  values (?,?) ", DBName, "zorm001", time.Now(), 100)
	_, err := zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to insert zorm001, err: %v", err)
	}

	finder = zorm.NewFinder()
	sql := fmt.Sprintf("insert into ?.?  values (%s,?)", "now")
	finder.Append(sql, DBName, "zorm005", "ZORM")
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("failed to insert zorm005, err: %v", err)
	}
}

//查询单条到结构体
func Test_query(t *testing.T) {
	defer dbDao.CloseDB()
	var demo Demo
	finder := zorm.NewFinder()
	//请确保查询结构只有一条
	finder.Append(fmt.Sprintf("select time ,value from %s.%s where value > ?", DBName, "zorm006"), 188)
	_, err := zorm.QueryRow(ctx, finder, &demo)
	if err != nil {
		t.Errorf("%v", err)
	}
	t.Logf("time:%v value :%v", demo.Time, demo.Value)

	var testBinary TestBinary
	finder = zorm.NewFinder()
	finder.Append(fmt.Sprintf("select time ,value from %s.%s where value > ?", DBName, "zorm005"), "ZORM")

	_, err = zorm.QueryRow(ctx, finder, &testBinary)
	if err != nil {
		t.Errorf("%v", err)
	}
	t.Logf("time:%v value :%v", testBinary.Time, testBinary.Value)
}

//Test_Query_rows 查询多条到结构体
func Test_query_rows(t *testing.T) {
	defer dbDao.CloseDB()
	var demos = make([]Demo, 0)
	finder := zorm.NewFinder()
	finder.Append(fmt.Sprintf("select time ,value from %s.%s order by time desc", DBName, "zorm001"))
	page := zorm.NewPage()
	page.PageSize = 2
	page.PageNo = 1
	// 如果是全部查询传nil
	err := zorm.Query(ctx, finder, &demos, page)
	//err := zorm.Query(ctx, finder, &demos, nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	for _, v := range demos {
		t.Logf("time:%v value :%v", v.Time, v.Value)
	}
}

//Test_query_map 查询单条到map
func Test_query_map(t *testing.T) {
	defer dbDao.CloseDB()
	finder := zorm.NewFinder()
	finder.Append(fmt.Sprintf("select time ,value from %s.%s where value > ?", DBName, "zorm006"), 188)
	demos, err := zorm.QueryRowMap(ctx, finder)
	if err != nil {
		t.Errorf("%v", err)
	}
	t.Logf("time:%v value :%v", demos["time"], demos["value"])
}

//Test_query_maps查询多条到map
func Test_query_maps(t *testing.T) {
	defer dbDao.CloseDB()
	finder := zorm.NewFinder()

	finder.Append(fmt.Sprintf("select time ,value from %s.%s order by time desc", DBName, "zorm001"))
	demos, err := zorm.QueryMap(ctx, finder, nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	for _, v := range demos {
		t.Logf("time:%v value :%v", v["time"], v["value"])
	}
}

// Test_drop_database 删除数据库
func Test_drop_database(t *testing.T) {
	defer dbDao.CloseDB()
	finder := zorm.NewFinder()
	finder.Append("drop database zorm")
	_, err := zorm.UpdateFinder(ctx, finder)
	if err != nil {
		t.Errorf("%v", err)
	}
}
