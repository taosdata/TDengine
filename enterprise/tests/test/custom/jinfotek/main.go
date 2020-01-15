// This is the GO version of the Jinfo importer.
// It is about 4x faster than the C/C++ version,
// because it utilize more CPU cores while the
// C/C++ version can only use 1 CPU core. This
// version also generate better diagnosis messages.
//
// But it has NOT been fully tested, so just keep
// both versions here.
//
// NOTE the .cpp files must be removed to make the
// GO compilable.

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DATA_CATEGORY_RD = iota
	DATA_CATEGORY_CDR
	DATA_CATEGORY_EVT
	DATA_CATEGORY_SMS
)

const (
	TS_2000_01_01   = 946684800
	MAX_OUTPUT_FILE = 20
	MAX_RECORD      = 1024 * 1024
)

type Record struct {
	catchtime int64
	Type      uint8
	tbname    string
	equid     string
	values    string
}

type InputFile struct {
	ts       uint32
	category int8
	path     string
}

type OutputFile struct {
	numRecord uint32
	file      *os.File
	bw        *bufio.Writer
}

var (
	g_inPath   = "."
	g_outPath  = "."
	g_database = "wjz"
	g_stable   = "tb_event"
)

func buildFileList() ([]InputFile, error) {
	var inputFiles []InputFile
	//    CDR.20190808154420_1565250270.1565250411.1000.dat
	// Dx_CDR.20190808144000_1565246700.0001.dat
	// Dx_EVT.20190808152500_1565249400.0011.dat
	//    EVT.20190808152730_1565249260.1565249253.0001.dat
	// Dx_SMS.20190808135000_1565243700.0001.dat
	//    SMS.20190808154440_1565250290.1565250292.0001.dat
	// wjzEvt.6262410510020000_19700101137600.1565250354.828367.dat

	now := uint32(time.Now().Unix())
	p2c := []struct {
		prefix   string
		category int8
	}{
		{"WJZEVT.", DATA_CATEGORY_RD},
		{"CDR.", DATA_CATEGORY_CDR},
		{"DX_CDR.", DATA_CATEGORY_CDR},
		{"EVT.", DATA_CATEGORY_EVT},
		{"DX_EVT.", DATA_CATEGORY_EVT},
		{"SMS.", DATA_CATEGORY_SMS},
		{"DX_SMS.", DATA_CATEGORY_SMS},
	}

	getTimestamp := func(name string, category int8) uint32 {
		i := strings.IndexByte(name, '.')
		if i == -1 {
			return 0
		}
		name = name[i+1:]

		i = strings.IndexByte(name, '_')
		if i == -1 {
			return 0
		}
		name = name[i+1:]

		if category == DATA_CATEGORY_RD {
			i = strings.IndexByte(name, '.')
			if i == -1 {
				return 0
			}
			name = name[i+1:]
		}

		i = strings.IndexByte(name, '.')
		if i == -1 {
			return 0
		}
		name = name[:i]

		if ts, e := strconv.ParseUint(name, 10, 32); e == nil {
			return uint32(ts)
		}

		return 0
	}

	walk := func(path string, fi os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if fi.IsDir() {
			return nil
		}
		name := strings.ToUpper(fi.Name())
		if len(name) < 41 {
			return nil
		}
		if !strings.HasSuffix(name, ".DAT") {
			return nil
		}

		inf := InputFile{path: path, category: -1}
		for i := 0; i < len(p2c); i++ {
			c := &p2c[i]
			if strings.HasPrefix(name, c.prefix) {
				inf.category = c.category
				break
			}
		}

		if inf.category == -1 {
			return nil
		}

		inf.ts = getTimestamp(name, inf.category)
		if inf.ts < TS_2000_01_01 || inf.ts > now {
			fmt.Println("invalid timestamp, skip: ", path)
			return nil
		}

		inf.path, _ = filepath.Rel(g_inPath, path)
		inputFiles = append(inputFiles, inf)
		return nil
	}

	inputFiles = make([]InputFile, 0, 1024*1024)
	if e := filepath.Walk(g_inPath, walk); e != nil {
		fmt.Println("failed to build input file list:", e.Error())
		return nil, e
	}

	sort.Slice(inputFiles, func(i, j int) bool {
		return inputFiles[i].ts < inputFiles[j].ts
	})
	return inputFiles, nil
}

var sendToRecordWriter func(records []*Record)
var stopRecordWriter func()

func startRecordWriter() error {
	ch := make(chan []*Record)
	wg := sync.WaitGroup{}
	outputFiles := make([]OutputFile, 0, MAX_OUTPUT_FILE)
	tableFile := (*os.File)(nil)
	tables := make(map[string]*OutputFile)

	createFiles := func() error {
		path := filepath.Join(g_outPath, "tables.sql")
		f, e := os.Create(path)
		if e != nil {
			fmt.Println("failed to create tables.sql:", e.Error())
			return e
		}
		f.WriteString(fmt.Sprintf("use %s;\n", g_database))
		tableFile = f

		for i := 0; i < MAX_OUTPUT_FILE; i++ {
			path = filepath.Join(g_outPath, fmt.Sprintf("%04d.sql", i))
			f, e = os.Create(path)
			if e != nil {
				fmt.Printf("failed to create output file: %s: %s\n", e.Error(), path)
				return e
			}
			bw := bufio.NewWriter(f)
			bw.WriteString(fmt.Sprintf("use %s;\n", g_database))
			outputFiles = append(outputFiles, OutputFile{file: f, bw: bw})
		}

		return nil
	}

	closeFiles := func() {
		for _, of := range outputFiles {
			if of.numRecord%10 != 0 {
				of.bw.WriteByte(';')
			}
			of.bw.Flush()
			of.file.Close()
		}

		if tableFile != nil {
			tableFile.Close()
		}
	}

	saveRecord := func(r *Record) error {
		file := tables[r.tbname]
		if file == nil {
			const format = "CREATE TABLE %s USING %s TAGS ('%s', '', '%d', '');\n"
			file = &outputFiles[len(tables)%len(outputFiles)]
			sql := fmt.Sprintf(format, r.tbname, g_stable, r.equid, r.Type)
			if _, e := tableFile.WriteString(sql); e != nil {
				return e
			}
			tables[r.tbname] = file
		}

		bw := file.bw
		if file.numRecord%10 == 0 {
			if _, e := bw.WriteString("insert into"); e != nil {
				return e
			}
		}
		if e := bw.WriteByte(' '); e != nil {
			return e
		}
		if _, e := bw.WriteString(r.tbname); e != nil {
			return e
		}
		if _, e := bw.WriteString(" values("); e != nil {
			return e
		}
		if _, e := bw.WriteString(strconv.FormatInt(r.catchtime, 10)); e != nil {
			return e
		}
		if _, e := bw.WriteString(r.values); e != nil {
			return e
		}

		file.numRecord++
		if file.numRecord%10 == 0 {
			if _, e := bw.WriteString(";\n"); e != nil {
				return e
			}
		}

		return nil
	}

	sendToRecordWriter = func(records []*Record) {
		ch <- records
	}

	stopRecordWriter = func() {
		close(ch)
		wg.Wait()
	}

	if e := createFiles(); e != nil {
		return e
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer closeFiles()

		last := &Record{}
		for records, ok := <-ch; ok; records, ok = <-ch {
			for _, r := range records {
				if r.catchtime <= last.catchtime && r.equid == last.equid {
					last.catchtime++
					r.catchtime = last.catchtime
				} else {
					last = r
				}
				if e := saveRecord(r); e != nil {
					fmt.Println("failed to save record:", e.Error())
					os.Exit(4)
				}
			}
		}
		fmt.Println("time for writting", duration)
	}()

	return nil
}

func saveRecords(records []*Record, saveAll bool) int32 {
	sort.Slice(records, func(i, j int) bool {
		x, y := records[i], records[j]
		if x.catchtime != y.catchtime {
			return y.catchtime < x.catchtime
		}
		return strings.Compare(x.equid, y.equid) < 0
	})

	first := 0
	if !saveAll && len(records) > 0 {
		first = len(records) / 5
		for ts := records[first].catchtime; first > 0; first-- {
			if records[first-1].catchtime != ts {
				break
			}
		}
	}

	rs := make([]*Record, len(records)-first)
	for i, j := 0, len(records)-1; i < len(rs); i++ {
		rs[i] = records[j]
		records[j] = nil
		j--
	}

	sendToRecordWriter(rs)
	return int32(first)
}

func parseFiles(inputFiles []InputFile) (e error) {
	type job struct {
		path   string
		lines  []string
		parser func(str string) (*Record, error)
	}

	parsers := []func(str string) (*Record, error){
		rdParseRecord,
		cdrParseRecord,
		evtParseRecord,
		smsParseRecord,
	}

	var wgFileParser, wgJob sync.WaitGroup
	var numRecords int32
	ch := make(chan job, 32)
	records := make([]*Record, MAX_RECORD)
	now := time.Now().Unix()

	fileParser := func() {
		for job, ok := <-ch; ok; job, ok = <-ch {
			for i, line := range job.lines {
				if r, e := job.parser(line); e != nil {
					fmt.Printf("%s (%d): %s.\n", job.path, i+1, e.Error())
				} else if r.catchtime < TS_2000_01_01 || r.catchtime > now {
					fmt.Printf("%s (%d): invalid timestamp.\n", job.path, i+1)
				} else {
					r.catchtime *= 1000
					idx := atomic.AddInt32(&numRecords, 1) - 1
					records[idx] = r
				}
			}
			wgJob.Done()
		}
		wgFileParser.Done()
	}

	ch = make(chan job, 32)
	for i := 0; i < 10; i++ {
		wgFileParser.Add(1)
		go fileParser()
	}

	var totalLines int
	for _, f := range inputFiles {
		path := filepath.Join(g_inPath, f.path)
		file, err := os.Open(path)
		if err != nil {
			e = err
			break
		}

		scanner := bufio.NewScanner(file)
		job := job{path: f.path, parser: parsers[f.category]}
		for scanner.Scan() {
			job.lines = append(job.lines, scanner.Text())
		}
		file.Close()
		if e = scanner.Err(); e == io.EOF {
			e = nil
		} else if e != nil {
			break
		}

		totalLines += len(job.lines)
		if totalLines > MAX_RECORD {
			wgJob.Wait()
			num := atomic.LoadInt32(&numRecords)
			num = saveRecords(records[:num], false)
			atomic.StoreInt32(&numRecords, num)
			totalLines = int(num) + len(job.lines)
		}

		wgJob.Add(1)
		ch <- job
	}

	close(ch)
	wgFileParser.Wait()

	if e == nil {
		num := atomic.LoadInt32(&numRecords)
		saveRecords(records[:num], true)
	}
	return e
}

func parseArguments() {
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-in=") {
			g_inPath = arg[4:]
			continue
		}
		if strings.HasPrefix(arg, "-out=") {
			g_outPath = arg[5:]
			continue
		}
		if strings.HasPrefix(arg, "-db=") {
			g_database = arg[4:]
			continue
		}
		if strings.HasPrefix(arg, "-stable=") {
			g_stable = arg[8:]
			continue
		}
		fmt.Println("USAGE: jinfo -in=<input path> -out=<output path> [-db=<database>] [-stable=<stable>]")
		os.Exit(1)
	}
}

func main() {
	parseArguments()

	inputFiles, e := buildFileList()
	if e != nil {
		os.Exit(2)
	}
	fmt.Println(len(inputFiles), "input files")

	e = startRecordWriter()
	if e != nil {
		os.Exit(3)
	}

	e = parseFiles(inputFiles)
	if e != nil {
		os.Exit(4)
	}

	stopRecordWriter()
	os.Exit(0)
}
