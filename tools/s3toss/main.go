package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var config struct {
	BlockSize int64
	DNode     uint
	DataDirs  [3][]string
	Endpoint  string
	Secure    bool
	AccessKey string
	SecretKey string
	Bucket    string
	Region    string
}

var minioClient *minio.Client

func createMinIOClient() {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: config.Secure,
		Region: config.Region,
	})
	if err != nil {
		log.Fatalln(err)
	}
	minioClient = client
	log.Println("MinIO S3 client initialized successfully.")
}

type File struct {
	DidLevel uint   `json:"did.level"`
	DidID    uint   `json:"did.id"`
	LCN      int    `json:"lcn"`
	FID      uint   `json:"fid"`
	MID      uint   `json:"mid"`
	CID      uint   `json:"cid"`
	Size     uint64 `json:"size"`
	MinVer   uint64 `json:"minVer"`
	MaxVer   uint64 `json:"maxVer"`
}

type SttFile struct {
	File
	Level uint `json:"level"`
}

type SttLvl struct {
	Level uint       `json:"level"`
	Files []*SttFile `json:"files"`
}

type FileSet struct {
	FID         uint      `json:"fid"`
	Head        *File     `json:"head,omitempty"`
	Data        *File     `json:"data,omitempty"`
	Sma         *File     `json:"sma,omitempty"`
	Tomb        *File     `json:"tomb,omitempty"`
	SttLvl      []*SttLvl `json:"stt lvl"`
	LastCompact uint64    `json:"last compact"`
	LastCommit  uint64    `json:"last commit"`
	LastMigrate uint64    `json:"last migrate"`
}

type Manifest struct {
	FmtV  uint `json:"fmtv"`
	DNode uint `json:"dnode"`
	VNode uint `json:"vnode"`
	*FileSet
}

func localFileName(vid uint, f *File, ext string) string {
	if f.LCN >= 1 {
		if ext != "data" {
			panic("LCN > 1 but ext is not data")
		} else if f.MID > 0 {
			return fmt.Sprintf("v%df%dver%d.m%d.%d.%s", vid, f.FID, f.CID, f.MID, f.LCN, ext)
		} else {
			return fmt.Sprintf("v%df%dver%d.%d.%s", vid, f.FID, f.CID, f.LCN, ext)
		}
	} else {
		if f.MID > 0 {
			return fmt.Sprintf("v%df%dver%d.m%d.%s", vid, f.FID, f.CID, f.MID, ext)
		} else {
			return fmt.Sprintf("v%df%dver%d.%s", vid, f.FID, f.CID, ext)
		}
	}
}

func localFilePath(vid uint, f *File, ext string) string {
	return filepath.Join(config.DataDirs[f.DidLevel][f.DidID],
		"vnode",
		fmt.Sprintf("vnode%d", vid),
		"tsdb",
		localFileName(vid, f, ext),
	)
}

func uploadFile(vid uint, f *File, ext string) error {
	path := localFilePath(vid, f, ext)
	objName := fmt.Sprintf("vnode%d/f%d/v%df%dver%d.m1.%s", vid, f.FID, vid, f.FID, f.CID, ext)
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	_, err := minioClient.FPutObject(context.Background(), config.Bucket, objName, path, opts)
	return err
}

func downloadFile(vid uint, f *File, ext string) error {
	path := localFilePath(vid, f, ext)
	objName := fmt.Sprintf("vnode%d/f%d/v%df%dver%d.m%d.%s", vid, f.FID, vid, f.FID, f.CID, f.MID, ext)
	opts := minio.GetObjectOptions{}
	return minioClient.FGetObject(context.Background(), config.Bucket, objName, path, opts)
}

func downloadLastChunk(vid uint, f *File) error {
	path := localFilePath(vid, f, "data")
	objName := fmt.Sprintf("vnode%d/f%d/v%df%dver%d.m%d.%d.data", vid, f.FID, vid, f.FID, f.CID, f.MID, f.LCN)
	opts := minio.GetObjectOptions{}
	return minioClient.FGetObject(context.Background(), config.Bucket, objName, path, opts)
}

func uploadDataFile(vid uint, f *File) (int, error) {
	// copy remote data blocks to the correct new location
	for i := 1; i < f.LCN; i++ {
		log.Printf("Starting copy remote data block, cn = %d\n", i)
		srcOpts := minio.CopySrcOptions{
			Bucket: config.Bucket,
			Object: fmt.Sprintf("%d/v%df%dver%d.%d.data", config.DNode, vid, f.FID, f.CID, i),
		}
		dstOpts := minio.CopyDestOptions{
			Bucket: config.Bucket,
			Object: fmt.Sprintf("vnode%d/f%d/v%df%dver%d.%d.data", vid, f.FID, vid, f.FID, f.CID, i),
		}
		_, err := minioClient.CopyObject(context.Background(), dstOpts, srcOpts)
		if err != nil {
			log.Printf("Failed to copy remote data block %d: %v\n", i, err)
			return 0, err
		}
	}

	// upload local data blocks
	path := localFilePath(vid, f, "data")
	fdata, err := os.Open(path)
	if err != nil {
		log.Printf("Failed to open local data file: %v\n", err)
		return 0, err
	}
	defer fdata.Close()

	cn := f.LCN
	offset, fileSize, chunkSize := int64(0), int64(0), config.BlockSize
	if fi, err := fdata.Stat(); err != nil {
		log.Printf("Failed to stat local data file: %v\n", err)
		return 0, err
	} else {
		fileSize = fi.Size()
	}

	for offset < fileSize {
		log.Printf("Starting upload data block, cn = %d\n", cn)

		// must call Seek, the below PutObject does not change the offset
		if _, err = fdata.Seek(offset, io.SeekStart); err != nil {
			log.Printf("Failed to seek in local data file: %v\n", err)
			return 0, err
		}

		objName := fmt.Sprintf("vnode%d/f%d/v%df%dver%d.%d.data", vid, f.FID, vid, f.FID, f.CID, cn)
		if chunkSize >= fileSize-offset {
			chunkSize = fileSize - offset
			objName = fmt.Sprintf("vnode%d/f%d/v%df%dver%d.m1.%d.data", vid, f.FID, vid, f.FID, f.CID, cn)
		}

		opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
		_, err = minioClient.PutObject(context.Background(), config.Bucket, objName, fdata, chunkSize, opts)
		if err != nil {
			log.Printf("Failed to upload data block %d: %v\n", cn, err)
			return 0, err
		}
		offset += chunkSize
		cn++
	}
	cn--

	// create a new local last chunk file if necessary
	if cn > f.LCN {
		log.Printf("Starting create new local last chunk file")
		f1 := *f
		f1.LCN = cn
		path = localFilePath(vid, &f1, "data")
		fdata.Seek(offset-chunkSize, io.SeekStart)

		fnew, err := os.Create(path)
		if err != nil {
			log.Printf("Failed to create new local last chunk file: %v\n", err)
			return 0, err
		}
		defer fnew.Close()
		if _, err = io.Copy(fnew, fdata); err != nil {
			log.Printf("Failed to write new local last chunk file: %v\n", err)
			return 0, err
		}
	}

	return cn, nil
}

// uploadManifest generates and uploads manifests.json to remote, note this function
// should NOT modify the input fset.
func uploadManifest(vid uint, fset *FileSet, lcn int, now time.Time) error {
	manifest := &Manifest{
		FmtV:  1,
		DNode: config.DNode,
		VNode: vid,
		FileSet: &FileSet{
			FID:         fset.FID,
			Head:        &File{},
			Data:        &File{},
			Sma:         &File{},
			SttLvl:      []*SttLvl{},
			LastCommit:  fset.LastCommit,
			LastCompact: fset.LastCompact,
			LastMigrate: uint64(now.UnixMilli()),
		},
	}

	*manifest.Head = *fset.Head
	manifest.Head.MID = 1

	*manifest.Data = *fset.Data
	manifest.Data.MID = 1
	manifest.Data.LCN = lcn

	*manifest.Sma = *fset.Sma
	manifest.Sma.MID = 1

	if fset.Tomb != nil {
		manifest.Tomb = &File{}
		*manifest.Tomb = *fset.Tomb
		manifest.Tomb.MID = 1
	}

	for _, lvl := range fset.SttLvl {
		newLvl := &SttLvl{Level: lvl.Level}
		for _, stt := range lvl.Files {
			newStt := *stt
			newStt.MID = 1
			newLvl.Files = append(newLvl.Files, &newStt)
		}
		manifest.SttLvl = append(manifest.SttLvl, newLvl)
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	objName := fmt.Sprintf("vnode%d/f%d/manifests.json", vid, fset.FID)
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	_, err = minioClient.PutObject(context.Background(), config.Bucket, objName, bytes.NewReader(data), int64(len(data)), opts)
	return err
}

func leaderMigrateFileSet(vid uint, fset *FileSet) bool {
	log.Printf("Migrating file set %d of vnode %d...\n", fset.FID, vid)

	if fset.Head == nil || fset.Data == nil || fset.Sma == nil {
		log.Println("Missing required files, skipping migration.")
		return false
	}

	if fset.Data.LCN < 1 {
		log.Println("LCN of data is less than 1, skipping migration.")
		return false
	}

	manifestPath := fmt.Sprintf("vnode%d/f%d/manifests.json", vid, fset.FID)
	_, err := minioClient.StatObject(context.Background(), config.Bucket, manifestPath, minio.StatObjectOptions{})
	if err == nil {
		log.Println("Remote manifests.json already exists, skipping migration.")
		return false
	}
	if er := minio.ToErrorResponse(err); er.Code != minio.NoSuchKey {
		log.Println("Failed to check remote manifests.json:", er.Message)
		return false
	}

	log.Println("Starting upload HEAD file.")
	if err := uploadFile(vid, fset.Head, "head"); err != nil {
		log.Printf("Failed to upload HEAD file: %v\n", err)
		return false
	}

	log.Println("Starting upload SMA file.")
	if err := uploadFile(vid, fset.Sma, "sma"); err != nil {
		log.Printf("Failed to upload SMA file: %v\n", err)
		return false
	}

	if fset.Tomb != nil {
		log.Println("Starting upload TOMB file.")
		if err := uploadFile(vid, fset.Tomb, "tomb"); err != nil {
			log.Printf("Failed to upload TOMB file: %v\n", err)
			return false
		}
	}

	log.Println("Starting upload STT files.")
	for _, lvl := range fset.SttLvl {
		for _, stt := range lvl.Files {
			if err := uploadFile(vid, &stt.File, "stt"); err != nil {
				log.Printf("Failed to upload STT file at level %d commit %d: %v\n", lvl.Level, stt.CID, err)
				return false
			}
		}
	}

	lcn, err := uploadDataFile(vid, fset.Data)
	if err != nil {
		return false
	}

	now := time.Now()
	log.Println("Starting upload manifests.json.")
	if err = uploadManifest(vid, fset, lcn, now); err != nil {
		log.Printf("Failed to upload manifests.json: %v\n", err)
		return false
	}

	// all succeeded, we can now update the file set
	fset.Data.LCN = lcn
	fset.LastMigrate = uint64(now.UnixMilli())
	log.Printf("Migration of file set %d of vnode %d succeed.\n", fset.FID, vid)
	return true
}

func followerMigrateFileSet(vid uint, fset *FileSet) bool {
	log.Printf("Migrating file set %d of vnode %d...\n", fset.FID, vid)

	manifestPath := fmt.Sprintf("vnode%d/f%d/manifests.json", vid, fset.FID)
	obj, err := minioClient.GetObject(context.Background(), config.Bucket, manifestPath, minio.GetObjectOptions{})
	if err != nil {
		log.Printf("Failed to get remote manifests.json: %v\n", err)
		return false
	}

	var manifest Manifest
	if err = json.NewDecoder(obj).Decode(&manifest); err != nil {
		log.Printf("Failed to decode remote manifests.json: %v\n", err)
		return false
	}

	if manifest.Head == nil || manifest.Data == nil || manifest.Sma == nil {
		log.Println("Missing required remote files, skipping migration.")
		return false
	}

	log.Println("Starting download HEAD file.")
	if err = downloadFile(vid, manifest.Head, "head"); err != nil {
		log.Printf("Failed to download HEAD file: %v\n", err)
		return false
	}

	log.Println("Starting download SMA file.")
	if err = downloadFile(vid, manifest.Sma, "sma"); err != nil {
		log.Printf("Failed to download SMA file: %v\n", err)
		return false
	}

	if manifest.Tomb != nil {
		log.Println("Starting download TOMB file.")
		if err = downloadFile(vid, manifest.Tomb, "tomb"); err != nil {
			log.Printf("Failed to download TOMB file: %v\n", err)
			return false
		}
	}

	log.Println("Starting download STT files.")
	for _, lvl := range manifest.SttLvl {
		for _, stt := range lvl.Files {
			if err := downloadFile(vid, &stt.File, "stt"); err != nil {
				log.Printf("Failed to download STT file at level %d commit %d: %v\n", lvl.Level, stt.CID, err)
				return false
			}
		}
	}

	log.Println("Starting download last chunk of DATA file.")
	if err = downloadLastChunk(vid, manifest.Data); err != nil {
		log.Printf("Failed to download last chunk of DATA file: %v\n", err)
		return false
	}

	// all succeeded, we can now update the file set
	fset.Head = manifest.Head
	fset.Data = manifest.Data
	fset.Sma = manifest.Sma
	fset.Tomb = manifest.Tomb
	fset.SttLvl = manifest.SttLvl
	fset.LastCompact = manifest.LastCompact
	fset.LastCommit = manifest.LastCommit
	fset.LastMigrate = manifest.LastMigrate
	log.Printf("Migration of file set %d of vnode %d succeed.\n", fset.FID, vid)
	return true
}

func legacyMigrateFileSet(vid uint, fset *FileSet) bool {
	log.Printf("Migrating file set %d of vnode %d...\n", fset.FID, vid)

	f := fset.Data
	if f == nil {
		log.Println("Data file not found, skipping migration.")
		return false
	}
	if f.LCN > 0 {
		log.Println("LCN of data is greater than 0, skipping migration.")
		return false
	}

	path := localFilePath(vid, fset.Data, "data")
	fdata, err := os.Open(path)
	if err != nil {
		log.Printf("Failed to open local data file: %v\n", err)
		return false
	}
	defer fdata.Close()

	offset, fileSize := int64(0), int64(0)
	if fi, err := fdata.Stat(); err != nil {
		log.Printf("Failed to stat local data file: %v\n", err)
		return false
	} else if fileSize = fi.Size(); fileSize <= config.BlockSize {
		log.Println("Data file too small, skipping migration.")
		return false
	}

	cn := 1
	for config.BlockSize < fileSize-offset {
		log.Printf("Starting upload data block, cn = %d\n", cn)

		objName := fmt.Sprintf("%d/v%df%dver%d.%d.data", config.DNode, vid, f.FID, f.CID, cn)
		opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
		_, err = minioClient.PutObject(context.Background(), config.Bucket, objName, fdata, config.BlockSize, opts)
		if err != nil {
			log.Printf("Failed to upload data block %d: %v\n", cn, err)
			return false
		}

		cn++
		offset += config.BlockSize
		// must call Seek, the above PutObject does not change the offset
		if _, err = fdata.Seek(offset, io.SeekStart); err != nil {
			log.Printf("Failed to seek in local data file: %v\n", err)
			return false
		}
	}

	log.Printf("Starting create new local last chunk file")
	f1 := *f
	f1.LCN = cn
	path = localFilePath(vid, &f1, "data")

	fnew, err := os.Create(path)
	if err != nil {
		log.Printf("Failed to create new local last chunk file: %v\n", err)
		return false
	}
	defer fnew.Close()
	if _, err = io.Copy(fnew, fdata); err != nil {
		log.Printf("Failed to write new local last chunk file: %v\n", err)
		return false
	}

	// all succeeded, we can now update the file set
	f.LCN = cn
	log.Printf("Migration of file set %d of vnode %d succeed.\n", fset.FID, vid)
	return true
}

func getFileList(vid uint, fsets []*FileSet) map[string]bool {
	result := map[string]bool{}

	for _, fset := range fsets {
		if fset.Head != nil {
			result[localFilePath(vid, fset.Head, "head")] = true
		}
		if fset.Sma != nil {
			result[localFilePath(vid, fset.Sma, "sma")] = true
		}
		if fset.Data != nil {
			result[localFilePath(vid, fset.Data, "data")] = true
		}
		if fset.Tomb != nil {
			result[localFilePath(vid, fset.Tomb, "tomb")] = true
		}
		for _, lvl := range fset.SttLvl {
			for _, stt := range lvl.Files {
				result[localFilePath(vid, &stt.File, "stt")] = true
			}
		}
	}

	return result
}

func migrateVnode(mode string, vid, fid uint) {
	log.Printf("Migrating vnode %d...\n", vid)

	var tsdb struct {
		FmtV uint       `json:"fmtv"`
		FSet []*FileSet `json:"fset"`
	}

	path := filepath.Join(config.DataDirs[0][0], "vnode", fmt.Sprintf("vnode%d", vid), "tsdb", "current.c.json")
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		log.Println("Unable to proceed: file current.c.json found")
		return
	}

	path = filepath.Join(config.DataDirs[0][0], "vnode", fmt.Sprintf("vnode%d", vid), "tsdb", "current.m.json")
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		log.Println("Unable to proceed: file current.m.json found")
		return
	}

	path = filepath.Join(config.DataDirs[0][0], "vnode", fmt.Sprintf("vnode%d", vid), "tsdb", "current.json")
	file, err := os.Open(path)
	if err != nil {
		log.Printf("Failed to open current.json of TSDB of vnode %d: %v", vid, err)
		return
	}

	err = json.NewDecoder(file).Decode(&tsdb)
	file.Close()

	if err != nil {
		log.Printf("Failed to decode current.json of TSDB of vnode %d: %v", vid, err)
		return
	}

	oldList := getFileList(vid, tsdb.FSet)
	succ := 0
	// Migrate file sets. Note, for all the 3 xxxMigrateFileSet functions, they can only
	// modify the input FileSet when they return true, otherwise a corrupted current.json
	// will be generated.
	for _, fset := range tsdb.FSet {
		if fid > 0 && fset.FID != fid {
			continue
		}

		switch mode {
		case "leader":
			// leader mode: works as the leader vnode in shared storage migration.
			// 1. copy legacy remote files to new location;
			// 2. upload local files to remote;
			// 3. generate new current.json;
			// 4. remove old local files.
			if leaderMigrateFileSet(vid, fset) {
				succ++
			}
		case "follower":
			// follower mode: works as the follower vnode in shared storage migration.
			// 1. download remote files to local;
			// 2. generate new current.json;
			// 3. remove old local files.
			if followerMigrateFileSet(vid, fset) {
				succ++
			}
		case "legacy":
			// legacy mode: works as a legacy vnode before shared storage support.
			// 1. upload local data blocks to remote;
			// 2. generate new current.json;
			// 3. remove old local files.
			// This is an internal mode for preparing test data.
			if legacyMigrateFileSet(vid, fset) {
				succ++
			}
		}
	}

	if succ == 0 {
		log.Printf("Migrating vnode %d finished, but no file set was migrated.\n", vid)
		return
	}

	newList := getFileList(vid, tsdb.FSet)

	// generate new current.json
	newPath := path + ".new"
	if file, err = os.Create(newPath); err != nil {
		log.Printf("Failed to create new current.json: %v\n", err)
		return
	}
	if err = json.NewEncoder(file).Encode(&tsdb); err != nil {
		file.Close()
		log.Printf("Failed to write new current.json: %v\n", err)
		return
	}
	file.Close()
	if err = os.Rename(newPath, path); err != nil {
		log.Printf("Failed to rename new current.json: %v\n", err)
		return
	}

	// remove all local files that are in oldList but not in newList
	for path := range oldList {
		if !newList[path] {
			if err := os.Remove(path); err != nil {
				log.Printf("Failed to remove old file %s: %v\n", path, err)
			}
		}
	}

	log.Printf("Migrating vnode %d finished.\n", vid)
}

func migrateVnodes(mode string, vid, fid uint) {
	if vid > 0 {
		migrateVnode(mode, vid, fid)
		return
	}

	entries, err := os.ReadDir(filepath.Join(config.DataDirs[0][0], "vnode"))
	if err != nil {
		log.Fatalf("Failed to list vnode directories: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		if !strings.HasPrefix(entry.Name(), "vnode") {
			continue
		}

		id, err := strconv.Atoi(entry.Name()[5:])
		if err != nil || id <= 0 {
			continue
		}

		migrateVnode(mode, uint(id), fid)
	}
}

func parseAccessString(as string) {
	items := strings.Split(as, ";")

	config.Endpoint = ""
	config.Bucket = ""
	config.Secure = true
	config.AccessKey = ""
	config.SecretKey = ""
	config.Region = ""

	for _, item := range items {
		part := strings.SplitN(item, "=", 2)
		switch strings.ToLower(part[0]) {
		case "endpoint":
			config.Endpoint = part[1]
		case "bucket":
			config.Bucket = part[1]
		case "protocol":
			config.Secure = !strings.EqualFold(part[1], "http")
		case "accesskeyid":
			config.AccessKey = part[1]
		case "secretaccesskey":
			config.SecretKey = part[1]
		case "region":
			config.Region = part[1]
		}
	}
}

func parseTaosCfg(path string) error {
	if fi, err := os.Stat(path); err != nil {
		return err
	} else if fi.IsDir() {
		path = filepath.Join(path, "taos.cfg")
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	ssConfig := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		switch strings.ToLower(parts[0]) {
		case "datadir":
			lvl := 0
			if len(parts) >= 3 {
				if lvl, err = strconv.Atoi(parts[2]); err != nil {
					return err
				}
			}
			config.DataDirs[lvl] = append(config.DataDirs[lvl], parts[1])

		case "ssAccessString":
			if strings.HasPrefix(parts[1], "s3:") {
				ssConfig = true
				parseAccessString(parts[1][3:])
			}

		case "s3endpoint":
			if ssConfig || config.Endpoint != "" {
				continue
			}
			if strings.HasPrefix(strings.ToLower(parts[1]), "http://") {
				config.Secure = false
				config.Endpoint = parts[1][7:]
			} else {
				config.Secure = true
				config.Endpoint = parts[1][8:]
			}

		case "s3accesskey":
			if ssConfig || config.AccessKey != "" {
				continue
			}
			if idx := strings.IndexByte(parts[1], ':'); idx == -1 {
				continue
			} else {
				config.AccessKey = parts[1][:idx]
				config.SecretKey = parts[1][idx+1:]
			}

		case "s3bucketname":
			if ssConfig || config.Bucket != "" {
				continue
			}
			config.Bucket = parts[1]
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func main() {
	var (
		taoscfg string
		mode    string
		vid     uint
		fid     uint
		blkSize int64
	)

	flag.StringVar(&taoscfg, "taoscfg", "", "Path to the TAOS configuration file, required")
	flag.StringVar(&mode, "mode", "", "Mode of the application, 'leader' or 'follower', required")
	flag.UintVar(&config.DNode, "dnode", 0, "ID of the dnode to be processed, required in leader mode")
	flag.UintVar(&vid, "vnode", 0, "ID of the vnode to be processed, default is processing all vnodes")
	flag.UintVar(&fid, "fset", 0, "ID of the file set to be processed, default is processing all file sets")
	flag.Int64Var(&blkSize, "blocksize", 512, "Block size of data file in MB")
	flag.Parse()

	// 'legacy' is an internal mode, for preparing test data
	if mode != "leader" && mode != "follower" && mode != "legacy" {
		log.Fatalln("Invalid mode specified, please use 'leader' or 'follower'")
	}

	if taoscfg == "" || (mode != "follower" && config.DNode == 0) {
		log.Fatalln("Missing required command line arguments.")
	}

	if blkSize <= 0 {
		log.Fatalln("Invalid block size.")
	}

	config.BlockSize = blkSize * 1024 * 1024
	if err := parseTaosCfg(taoscfg); err != nil {
		log.Fatalf("Failed to parse TAOS configuration file: %v\n", err)
	}

	createMinIOClient()

	log.Printf("Shared Storage upgrade tool is running in %s mode.\n", mode)
	migrateVnodes(mode, vid, fid)
	log.Println("Migration finished.")
}
