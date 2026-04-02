package unified

import "github.com/taosdata/driver-go/v3/ws/client"

type responseWithCodeAndMessage interface {
	GetCode() int
	GetMessage() string
}

func decodeJSONResponse(payload []byte, out interface{}) error {
	return client.JsonI.Unmarshal(payload, out)
}

func decodeAndCheckJSONResponse(payload []byte, out responseWithCodeAndMessage) error {
	if err := client.JsonI.Unmarshal(payload, out); err != nil {
		return err
	}
	return client.HandleResponseError(nil, out.GetCode(), out.GetMessage())
}

func decodeAndCheckJSONResponseAsProtocol(payload []byte, out responseWithCodeAndMessage, message string) error {
	if err := client.JsonI.Unmarshal(payload, out); err != nil {
		return &Error{
			Type:    ErrorTypeProtocol,
			Message: message,
			Cause:   err,
		}
	}
	return client.HandleResponseError(nil, out.GetCode(), out.GetMessage())
}
