package version

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	version := GetVersion()
	assert.Equal(t, Version, version)
	commitID := GetCommitID()
	assert.Equal(t, CommitID, commitID)
	CommitID = "1234567890"
	commitID = GetCommitID()
	assert.Equal(t, "1234567", commitID)
	buildDate := GetBuildDate()
	assert.Equal(t, BuildAt, buildDate)
	ShowVersion()
}
