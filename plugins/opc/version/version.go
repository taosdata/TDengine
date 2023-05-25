package version

import (
	"fmt"
	"time"
)

var Version = "1.0.0"
var BuildAt = time.Now().Format("20060102150405")
var CommitID = ""

func ShowVersion() string {
	return fmt.Sprintf("%s(build%s-%s)", Version, BuildAt, CommitID)
}
