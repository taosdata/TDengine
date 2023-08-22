package version

import (
	"fmt"
)

var Version = "1.0.0"
var BuildAt = ""
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
	return fmt.Sprintf("version: %s \r\ncommit id: %s \r\nbuild time %s", GetVersion(), GetCommitID(),
		GetBuildDate())
}
