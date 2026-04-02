package grafana_webhook

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

// HandleWebhook returns a http handler function
// h - HandlerFunc parameter is called after request successfully unmarshaled to the Body pointer
// bodyLimit - specifies a body size limit for the request, set 0 to unlimited
func HandleWebhook(h HandlerFunc, bodyLimit int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		// Grafana request body
		var b *Body

		// parse POST/ PUT values to the Grafana Body model
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			if bodyLimit > 0 {
				// set request body limit
				r.Body = http.MaxBytesReader(w, r.Body, bodyLimit)
			}
			reqData, e := ioutil.ReadAll(r.Body)
			if e != nil {
				// read body action has failed
				b = BodyOnReadAllSizeLimitErr()
			} else {
				json.Unmarshal(reqData, &b)
			}
			// run Grafana HandlerFunc
			if h != nil {
				h(w, b)
			}
		}
	}
}

type HandlerFunc func(w http.ResponseWriter, b *Body)
