package sqltype

import "testing"

func TestGetSqlType(t *testing.T) {
	tests := []struct {
		sql      string
		expected SqlType
	}{
		{"insert into table", InsertType},     // Starts with "insert"
		{"INSERT into table", InsertType},     // Uppercase "INSERT"
		{"  insert into table", InsertType},   // "insert" with leading whitespace
		{"select * from table", SelectType},   // Starts with "select"
		{"SELECT * from table", SelectType},   // Uppercase "SELECT"
		{"  select * from table", SelectType}, // "select" with leading whitespace
		{"delete from table", OtherType},      // Does not match "insert" or "select"
		{"ins", OtherType},                    // Less than 6 characters
		{"selec", OtherType},                  // Less than 6 characters
		{"", OtherType},                       // Empty string
		{"   ", OtherType},                    // Whitespace-only string
		{"插入数据", OtherType},                   // Non-ASCII characters
	}

	for _, test := range tests {
		result := GetSqlType(test.sql)
		if result != test.expected {
			t.Errorf("GetSqlType(%q) = %d; want %d", test.sql, result, test.expected)
		}
	}
}

func TestRemoveSpacesAndLowercase(t *testing.T) {
	type args struct {
		str       string
		byteCount int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "basic test",
			args: args{
				str:       "  Hello World  ",
				byteCount: 0,
			},
			want: "helloworld",
		},
		{
			name: "with byteCount less than string length",
			args: args{
				str:       "  Hello World  ",
				byteCount: 5,
			},
			want: "hello",
		},
		{
			name: "with byteCount more than string length",
			args: args{
				str:       "  Hello World  ",
				byteCount: 20,
			},
			want: "helloworld",
		},
		{
			name: "empty string",
			args: args{
				str:       "",
				byteCount: 0,
			},
			want: "",
		},
		{
			name: "string with only spaces",
			args: args{
				str:       "     ",
				byteCount: 0,
			},
			want: "",
		},
		{
			name: "string with mixed spaces and characters",
			args: args{
				str:       " A B C D E ",
				byteCount: 0,
			},
			want: "abcde",
		},
		{
			name: "string with mixed spaces and characters with byteCount",
			args: args{
				str:       " A B C D E ",
				byteCount: 3,
			},
			want: "abc",
		},
		{
			name: "string with no spaces",
			args: args{
				str:       "NoSpacesHere",
				byteCount: 0,
			},
			want: "nospaceshere",
		},
		{
			name: "string with no spaces and byteCount",
			args: args{
				str:       "NoSpacesHere",
				byteCount: 5,
			},
			want: "nospa",
		},
		{
			name: "string with special characters",
			args: args{
				str:       "  Hello, World!  ",
				byteCount: 0,
			},
			want: "hello,world!",
		},
		{
			name: "string with special characters and byteCount",
			args: args{
				str:       "  Hello, World!  ",
				byteCount: 7,
			},
			want: "hello,w",
		},
		{
			name: "string with newline and tab characters",
			args: args{
				str:       "\nHello\tWorld\n",
				byteCount: 0,
			},
			want: "helloworld",
		},
		{
			name: "string with newline and tab characters and byteCount",
			args: args{
				str:       "\nHello\tWorld\n",
				byteCount: 5,
			},
			want: "hello",
		},
		{
			name: "string with unicode spaces",
			args: args{
				str:       " Hello World ", // Note: these are non-breaking spaces (U+00A0)
				byteCount: 0,
			},
			want: "helloworld",
		},
		{
			name: "string with unicode spaces and byteCount",
			args: args{
				str:       " Hello World ", // Note: these are non-breaking spaces (U+00A0)
				byteCount: 5,
			},
			want: "hello",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RemoveSpacesAndLowercase(tt.args.str, tt.args.byteCount); got != tt.want {
				t.Errorf("RemoveSpacesAndLowercase() = %v, want %v", got, tt.want)
			}
		})
	}
}
