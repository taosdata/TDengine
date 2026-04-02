package wstool

import "testing"

func TestNotifyHandlesInitAndPut(t *testing.T) {
	var n NotifyHandles

	n.InitNotifyHandles()
	if n.WhitelistChangeHandle == 0 {
		t.Fatal("WhitelistChangeHandle should be initialized")
	}
	if n.DropUserHandle == 0 {
		t.Fatal("DropUserHandle should be initialized")
	}
	if n.WhitelistChangeChan == nil {
		t.Fatal("WhitelistChangeChan should be initialized")
	}
	if n.DropUserChan == nil {
		t.Fatal("DropUserChan should be initialized")
	}

	whitelistHandle := n.WhitelistChangeHandle
	dropUserHandle := n.DropUserHandle
	whitelistChan := n.WhitelistChangeChan
	dropUserChan := n.DropUserChan

	// Init should be idempotent when handles already exist.
	n.InitNotifyHandles()
	if n.WhitelistChangeHandle != whitelistHandle {
		t.Fatal("WhitelistChangeHandle should not change after re-init")
	}
	if n.DropUserHandle != dropUserHandle {
		t.Fatal("DropUserHandle should not change after re-init")
	}
	if n.WhitelistChangeChan != whitelistChan {
		t.Fatal("WhitelistChangeChan should not change after re-init")
	}
	if n.DropUserChan != dropUserChan {
		t.Fatal("DropUserChan should not change after re-init")
	}

	n.PutNotifyHandles()
	if n.WhitelistChangeHandle != 0 {
		t.Fatal("WhitelistChangeHandle should be reset after PutNotifyHandles")
	}
	if n.DropUserHandle != 0 {
		t.Fatal("DropUserHandle should be reset after PutNotifyHandles")
	}
	if n.WhitelistChangeChan != nil {
		t.Fatal("WhitelistChangeChan should be reset after PutNotifyHandles")
	}
	if n.DropUserChan != nil {
		t.Fatal("DropUserChan should be reset after PutNotifyHandles")
	}
}

func TestNotifyHandlesPutWithoutInit(t *testing.T) {
	var n NotifyHandles
	n.PutNotifyHandles()

	if n.WhitelistChangeHandle != 0 || n.DropUserHandle != 0 {
		t.Fatal("handles should remain zero when PutNotifyHandles is called without init")
	}
	if n.WhitelistChangeChan != nil || n.DropUserChan != nil {
		t.Fatal("channels should remain nil when PutNotifyHandles is called without init")
	}
}
