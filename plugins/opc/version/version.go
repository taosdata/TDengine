package version

import (
	"fmt"
	"time"
)

var Version = "1.0.0"
var BuildAt = time.Now().Format("20060102")
var CommitID = ""

func ShowVersion() string {
	commitID := CommitID
	if len(commitID) > 7 {
		commitID = commitID[0:7]
	}
	return fmt.Sprintf("%s(build%s-%s)", Version, BuildAt, commitID)
}
