package monitor

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadUint(t *testing.T) {
	// Valid uint64 number in a file
	validPath := "valid_uint.txt"
	err := os.WriteFile(validPath, []byte("123456"), 0644)
	assert.NoError(t, err)
	defer func() {
		err := os.Remove(validPath)
		assert.NoError(t, err)
	}()

	result, err := readUint(validPath)
	if err != nil || result != 123456 {
		t.Errorf("readUint(%q) = %d, %v; want 123456, nil", validPath, result, err)
	}

	// Invalid path
	_, err = readUint("nonexistent_file.txt")
	if err == nil {
		t.Error("Expected an error for nonexistent file, but got nil")
	}
}

func TestParseUint(t *testing.T) {
	// Valid unsigned integer string
	result, err := parseUint("123456", 10, 64)
	if err != nil || result != 123456 {
		t.Errorf("parseUint(%q, 10, 64) = %d, %v; want 123456, nil", "123456", result, err)
	}

	// Negative value as a string, should return 0, nil
	result, err = parseUint("-123", 10, 64)
	if err != nil || result != 0 {
		t.Errorf("parseUint(%q, 10, 64) = %d, %v; want 0, nil", "-123", result, err)
	}

	// Out-of-range negative value, should return 0, nil
	outOfRangeNeg := strconv.FormatInt(-9223372036854775808, 10)
	result, err = parseUint(outOfRangeNeg, 10, 64)
	if err != nil || result != 0 {
		t.Errorf("parseUint(%q, 10, 64) = %d, %v; want 0, nil", outOfRangeNeg, result, err)
	}

	// Invalid format (non-numeric)
	_, err = parseUint("abc", 10, 64)
	if err == nil {
		t.Error("Expected error for non-numeric input, but got nil")
	}

	// Out-of-range positive number for uint64
	largeValue := strings.Repeat("9", 21) // a very large number
	_, err = parseUint(largeValue, 10, 64)
	if err == nil {
		t.Errorf("Expected error for input %q out of uint64 range, but got nil", largeValue)
	}
}
