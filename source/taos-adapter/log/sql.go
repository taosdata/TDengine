package log

import (
	"bytes"

	"github.com/sirupsen/logrus"
)

var sqlLogger = logrus.New()

type TaosSqlLogFormatter struct {
}

func (t *TaosSqlLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}
	b.Reset()
	b.WriteString(entry.Time.Format("01/02 15:04:05.000000"))
	b.WriteByte(' ')
	b.WriteString(ServerID)
	b.WriteByte(' ')
	b.WriteString(entry.Message)
	b.WriteByte('\n')
	return b.Bytes(), nil
}
