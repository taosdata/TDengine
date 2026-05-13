package testtool

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/taosdata/driver-go/v3/common"
)

func HTTPQuery(payload string) (*common.TDEngineRestfulResp, error) {
	body := strings.NewReader(payload)
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:6041/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http code: %d", resp.StatusCode)
	}
	return common.UnmarshalRestfulBody(resp.Body, 512)
}
