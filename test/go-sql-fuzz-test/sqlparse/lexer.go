package sqlparser

import (
	"bytes"
	"fmt"
	"sqlparser/tool"
	"strings"
)

const eofChar = 0x100

type Scanner struct {
	buf     []byte
	bufPos  int
	bufSize int

	lastToken []byte
	lastErr   error

	position int
	lastChar uint16

	ParseTree Statement
}

func NewScanner(sql string) *Scanner {
	buf := tool.StringToBytes(sql)
	return &Scanner{
		buf:     buf,
		bufPos:  0,
		bufSize: len(buf),
	}
}

func (s *Scanner) Lex(lval *yySymType) int {
	tokenType, tokenVal := s.Scan()
	lval.token = Token{
		Type:  tokenType,
		Bytes: tokenVal,
	}
	s.lastToken = tokenVal
	return tokenType
}

func (s *Scanner) Error(errStr string) {
	if errStr == "syntax error" && s.lastToken == nil {
		s.lastErr = fmt.Errorf("incomplete sql at position %v", s.bufPos)
		return
	}
	if s.lastToken != nil {
		s.lastErr = fmt.Errorf("%s at position %v near '%s'", errStr, s.bufPos, s.lastToken)
	} else {
		s.lastErr = fmt.Errorf("%s at position %v", errStr, s.bufPos)
	}
}

func (s *Scanner) Scan() (int, []byte) {
	if s.lastChar == 0 {
		s.next()
	}
	s.skipBlank()
	switch ch := s.lastChar; {
	case tool.IsLetter(ch):
		s.next()
		// keyword or identifier
		return s.scanIdentifier(byte(ch))
	case tool.IsDigit(ch):
		// number
		return s.scanNumber(false)
	default:
		s.next()
		switch ch {
		case eofChar:
			return 0, nil
		case '=':
			return NK_EQ, nil
		case ',':
			return NK_COMMA, nil
		case ';':
			return NK_SEMI, nil
		case '(':
			return NK_LP, nil
		case ')':
			return NK_RP, nil
		case '+':
			return NK_PLUS, nil
		case '*':
			return NK_STAR, nil
		case '%':
			return NK_REM, nil
		case '^':
			return int(ch), nil
		case '~':
			return NK_BITNOT, nil
		case '&':
			if s.lastChar == '&' {
				s.next()
				return AND, nil
			}
			return NK_BITAND, nil
		case '|':
			if s.lastChar == '|' {
				s.next()
				return OR, nil
			}
			return NK_BITOR, nil
		case '?':
			return NK_QUESTION, nil
		case '.':
			if tool.IsDigit(s.lastChar) {
				// number starting with decimal point
				return s.scanNumber(true)
			}
			return NK_DOT, nil
		case '/':
			if s.lastChar == '*' {
				// multi-line comment
				s.next()
				if s.lastChar == '+' {
					// hint comment
					return s.scanHint()
				}
				return s.scanComment()
			}
			return NK_SLASH, nil
		case '-':
			// '->' json extract operator
			if s.lastChar == '>' {
				s.next()
				return NK_ARROW, nil
			}
			return NK_MINUS, nil
		case ':':
			return NK_COLON, nil
		case '<':
			switch s.lastChar {
			case '>':
				s.next()
				return NK_NE, nil
			case '=':
				s.next()
				return NK_LE, nil
			case '<':
				s.next()
				return NK_LSHIFT, nil
			default:
				return NK_LT, nil
			}
		case '>':
			if s.lastChar == '=' {
				s.next()
				return NK_GE, nil
			} else if s.lastChar == '>' {
				s.next()
				return NK_RSHIFT, nil
			}
			return NK_GT, nil
		case '!':
			if s.lastChar == '=' {
				s.next()
				return NK_NE, nil
			}
			return int(ch), nil
		case '\'':
			return s.scanString(ch, NK_STRING)
		case '"':
			return s.scanString(ch, NK_ALIAS)
		case '`':
			return s.scanLiteralIdentifier()
		default:
			return LEX_ERROR, []byte{byte(ch)}
		}

	}
}

// next reads the next bytes from the input buffer.
func (s *Scanner) next() {
	if s.bufPos >= s.bufSize {
		if s.lastChar != eofChar {
			s.position += 1
			s.lastChar = eofChar
		}
	} else {
		s.position += 1
		s.lastChar = uint16(s.buf[s.bufPos])
		s.bufPos += 1
	}
}

func (s *Scanner) skipBlank() {
	ch := s.lastChar
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		s.next()
		ch = s.lastChar
	}
}

func (s *Scanner) scanIdentifier(firstByte byte) (int, []byte) {
	buffer := &bytes.Buffer{}
	buffer.WriteByte(firstByte)
	for tool.IsLetter(s.lastChar) || tool.IsDigit(s.lastChar) {
		buffer.WriteByte(byte(s.lastChar))
		s.next()
	}
	upper := bytes.ToUpper(buffer.Bytes())
	upperStr := tool.BytesToString(upper)
	loweredStr := strings.ToLower(upperStr)

	// Check for boolean literals
	if loweredStr == "true" || loweredStr == "false" {
		return NK_BOOL, []byte(loweredStr)
	}

	if keywordID, found := Keywords[upperStr]; found {
		return keywordID, buffer.Bytes()
	}
	return NK_ID, []byte(loweredStr)
}

func (s *Scanner) scanMantissa(base int, buffer *bytes.Buffer) {
	for tool.DigitVal(s.lastChar) < base {
		s.consumeNext(buffer)
	}
}

func (s *Scanner) scanNumber(seenDecimalPoint bool) (int, []byte) {
	token := NK_INTEGER
	buffer := &bytes.Buffer{}
	if seenDecimalPoint {
		token = NK_FLOAT
		buffer.WriteByte('.')
		s.scanMantissa(10, buffer)
	} else {
		// 0x construct.
		if s.lastChar == '0' {
			s.consumeNext(buffer)
			if s.lastChar == 'x' || s.lastChar == 'X' {
				token = NK_HEX
				s.consumeNext(buffer)
				s.scanMantissa(16, buffer)
				// A letter cannot immediately follow a number.
				if tool.IsLetter(s.lastChar) {
					return LEX_ERROR, buffer.Bytes()
				}

				return token, buffer.Bytes()
			}
			// 0b construct for binary
			if s.lastChar == 'b' || s.lastChar == 'B' {
				token = NK_BIN
				s.consumeNext(buffer)
				s.scanMantissa(2, buffer)
				// A letter cannot immediately follow a number.
				if tool.IsLetter(s.lastChar) {
					return LEX_ERROR, buffer.Bytes()
				}
				return token, buffer.Bytes()
			}
		}

		// normal number
		s.scanMantissa(10, buffer)

		// Fractional part.
		if s.lastChar == '.' {
			token = NK_FLOAT
			s.consumeNext(buffer)
			s.scanMantissa(10, buffer)
		}
	}

	// Exponent part.
	if s.lastChar == 'e' || s.lastChar == 'E' {
		token = NK_FLOAT
		s.consumeNext(buffer)
		if s.lastChar == '+' || s.lastChar == '-' {
			s.consumeNext(buffer)
		}
		s.scanMantissa(10, buffer)
	}

	if token == NK_INTEGER {
		switch s.lastChar {
		case 'b', 'B', 'u', 'U', 'a', 'A', 's', 'S', 'm', 'M', 'h', 'H', 'd', 'D', 'n', 'N', 'y', 'Y', 'w', 'W':
			buffer.WriteByte(byte(s.lastChar))
			s.next()
			return NK_VARIABLE, buffer.Bytes()
		}
	}
	// A letter cannot immediately follow a number.
	if tool.IsLetter(s.lastChar) {
		return LEX_ERROR, buffer.Bytes()
	}

	return token, buffer.Bytes()
}

func (s *Scanner) consumeNext(buffer *bytes.Buffer) {
	if s.lastChar == eofChar {
		// This should never happen.
		panic("unexpected EOF")
	}
	buffer.WriteByte(byte(s.lastChar))
	s.next()
}

func (s *Scanner) scanComment() (int, []byte) {
	buffer := &bytes.Buffer{}
	buffer.WriteString("/*")
	for {
		if s.lastChar == '*' {
			s.consumeNext(buffer)
			if s.lastChar == '/' {
				s.consumeNext(buffer)
				break
			}
			continue
		}
		if s.lastChar == eofChar {
			return LEX_ERROR, buffer.Bytes()
		}
		s.consumeNext(buffer)
	}
	return COMMENT, buffer.Bytes()
}

func (s *Scanner) scanHint() (int, []byte) {
	buffer := &bytes.Buffer{}
	buffer.WriteString("/*+")
	s.next()
	for {
		if s.lastChar == '*' {
			s.consumeNext(buffer)
			if s.lastChar == '/' {
				s.consumeNext(buffer)
				break
			}
			continue
		}
		if s.lastChar == eofChar {
			return LEX_ERROR, buffer.Bytes()
		}
		s.consumeNext(buffer)
	}
	hint := ExtractHint(buffer.String())
	return NK_HINT, []byte(hint)
}

func ExtractHint(sql string) string {
	sql = sql[3 : len(sql)-2]
	hint := strings.ToLower(strings.TrimSpace(sql))
	return hint
}

func (s *Scanner) scanString(delim uint16, typ int) (int, []byte) {
	var buffer bytes.Buffer
	for {
		ch := s.lastChar
		if ch == eofChar {
			// Unterminated string.
			return LEX_ERROR, buffer.Bytes()
		}

		if ch != delim && ch != '\\' {
			buffer.WriteByte(byte(ch))

			// Scan ahead to the next interesting character.
			start := s.bufPos
			for ; s.bufPos < s.bufSize; s.bufPos++ {
				ch = uint16(s.buf[s.bufPos])
				if ch == delim || ch == '\\' {
					break
				}
			}

			buffer.Write(s.buf[start:s.bufPos])
			s.position += s.bufPos - start

			if s.bufPos >= s.bufSize {
				// Reached the end of the buffer without finding a delim or
				// escape character.
				s.next()
				continue
			}

			s.bufPos++
			s.position++
		}
		s.next() // Read one past the delim or escape character.

		if ch == '\\' {
			if s.lastChar == eofChar {
				// String terminates mid escape character.
				return LEX_ERROR, buffer.Bytes()
			}
			if decodedChar := SQLDecodeMap[byte(s.lastChar)]; decodedChar == DontEscape {
				ch = s.lastChar
			} else {
				ch = uint16(decodedChar)
			}

		} else if ch == delim && s.lastChar != delim {
			// Correctly terminated string, which is not a double delim.
			break
		}

		buffer.WriteByte(byte(ch))
		s.next()
	}

	return typ, buffer.Bytes()
}

func (s *Scanner) scanLiteralIdentifier() (int, []byte) {
	buffer := &bytes.Buffer{}
	backTickSeen := false
	for {
		if backTickSeen {
			if s.lastChar != '`' {
				break
			}
			backTickSeen = false
			buffer.WriteByte('`')
			s.next()
			continue
		}
		// The previous char was not a backtick.
		switch s.lastChar {
		case '`':
			backTickSeen = true
		case eofChar:
			// Premature EOF.
			return LEX_ERROR, buffer.Bytes()
		default:
			buffer.WriteByte(byte(s.lastChar))
		}
		s.next()
	}
	if buffer.Len() == 0 {
		return LEX_ERROR, buffer.Bytes()
	}
	return NK_ID, buffer.Bytes()
}

var SQLEncodeMap [256]byte

// SQLDecodeMap is the reverse of SQLEncodeMap
var SQLDecodeMap [256]byte

var encodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'"':    '"',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

var DontEscape = byte(255)

func init() {
	for i := range SQLEncodeMap {
		SQLEncodeMap[i] = DontEscape
		SQLDecodeMap[i] = DontEscape
	}
	for i := range SQLEncodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			SQLEncodeMap[byte(i)] = to
			SQLDecodeMap[to] = byte(i)
		}
	}
}
