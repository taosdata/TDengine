package version

import (
	"fmt"
	"time"
)

var Version = "1.0.0"
var BuildAt = time.Now().Format("2006-01-02")
var CommitID = ""

func GetVersion() string {
	return Version
}

func GetCommitID() string {
	commitID := CommitID
	if len(commitID) > 7 {
		commitID = commitID[0:7]
	}
	return commitID
}

func GetBuildDate() string {
	return BuildAt
}

func ShowVersion() string {
	return fmt.Sprintf("version: %s \r\ncommit id: %s \r\nbuild date %s", GetVersion(), GetCommitID(),
		GetBuildDate())
}
