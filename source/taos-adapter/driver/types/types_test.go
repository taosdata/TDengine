package types

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

// @author: xftan
// @date: 2022/1/27 16:20
// @description: test null bool type Scan()
func TestNullBool_Scan(t *testing.T) {
	type fields struct {
		Inner bool
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "true",
			fields: fields{
				Inner: true,
				Valid: true,
			},
			args: args{
				value: true,
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: true,
				Valid: false,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: false,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullBool{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:20
// @description: test null bool type Value()
func TestNullBool_Value(t *testing.T) {
	type fields struct {
		Inner bool
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "ture",
			fields: fields{
				Inner: true,
				Valid: true,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "false",
			fields: fields{
				Inner: false,
				Valid: true,
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: false,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullBool{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:21
// @description: test null float32 type Scan()
func TestNullFloat32_Scan(t *testing.T) {
	type fields struct {
		Inner float32
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: float32(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullFloat32{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:21
// @description: test null float32 type String()
func TestNullFloat32_String(t *testing.T) {
	type fields struct {
		Inner float32
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullFloat32{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:21
// @description: test null float32 type Value()
func TestNullFloat32_Value(t *testing.T) {
	type fields struct {
		Inner float32
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    float32(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullFloat32{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:21
// @description: test null float64 type Scan()
func TestNullFloat64_Scan(t *testing.T) {
	type fields struct {
		Inner float64
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: float64(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullFloat64{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:22
// @description: test null float64 type String()
func TestNullFloat64_String(t *testing.T) {
	type fields struct {
		Inner float64
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullFloat64{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:22
// @description: test null float64 type Value()
func TestNullFloat64_Value(t *testing.T) {
	type fields struct {
		Inner float64
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    float64(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullFloat64{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:22
// @description: test null int16 type Scan()
func TestNullInt16_Scan(t *testing.T) {
	type fields struct {
		Inner int16
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: int16(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullInt16{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:23
// @description: test null int16 type String()
func TestNullInt16_String(t *testing.T) {
	type fields struct {
		Inner int16
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullInt16{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:23
// @description: test null int16 type Value()
func TestNullInt16_Value(t *testing.T) {
	type fields struct {
		Inner int16
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    int16(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullInt16{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:23
// @description: test null int32 type Scan()
func TestNullInt32_Scan(t *testing.T) {
	type fields struct {
		Inner int32
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: int32(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullInt32{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:25
// @description: test null int32 type String()
func TestNullInt32_String(t *testing.T) {
	type fields struct {
		Inner int32
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullInt32{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:04
// @description: test null int32 type Value()
func TestNullInt32_Value(t *testing.T) {
	type fields struct {
		Inner int32
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    int32(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullInt32{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:23
// @description: test null int64 type Scan()
func TestNullInt64_Scan(t *testing.T) {
	type fields struct {
		Inner int64
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: int64(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullInt64{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:47
// @description: test null int64 type String()
func TestNullInt64_String(t *testing.T) {
	type fields struct {
		Inner int64
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullInt64{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:19
// @description: test null int64 type Value()
func TestNullInt64_Value(t *testing.T) {
	type fields struct {
		Inner int64
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    int64(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullInt64{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:23
// @description: test null int8 type Scan()
func TestNullInt8_Scan(t *testing.T) {
	type fields struct {
		Inner int8
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: int8(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullInt8{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:48
// @description: test null int8 type String()
func TestNullInt8_String(t *testing.T) {
	type fields struct {
		Inner int8
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullInt8{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:19
// @description: test null int8 type Value()
func TestNullInt8_Value(t *testing.T) {
	type fields struct {
		Inner int8
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    int8(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullInt8{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:24
// @description: test null json type Scan()
func TestNullJson_Scan(t *testing.T) {
	type fields struct {
		Inner RawMessage
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "common",
			fields: fields{},
			args: args{
				value: []byte{'1', '2', '3'},
			},
			wantErr: false,
		},
		{
			name:   "error",
			fields: fields{},
			args: args{
				value: 123,
			},
			wantErr: true,
		},
		{
			name:   "nil",
			fields: fields{},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullJson{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:19
// @description: test null json type Value()
func TestNullJson_Value(t *testing.T) {
	type fields struct {
		Inner RawMessage
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: RawMessage("123"),
				Valid: true,
			},
			want:    RawMessage("123"),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: nil,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullJson{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:24
// @description: test null string type Scan()
func TestNullString_Scan(t *testing.T) {
	type fields struct {
		Inner string
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "common",
			fields: fields{},
			args: args{
				value: "123",
			},
			wantErr: false,
		},
		{
			name:   "error",
			fields: fields{},
			args: args{
				value: 123,
			},
			wantErr: true,
		},
		{
			name:   "nil",
			fields: fields{},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullString{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:19
// @description: test null string type Value()
func TestNullString_Value(t *testing.T) {
	type fields struct {
		Inner string
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: "123",
				Valid: true,
			},
			want:    "123",
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: "",
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullString{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:24
// @description: test null time type Scan()
func TestNullTime_Scan(t *testing.T) {
	type fields struct {
		Time  time.Time
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "time",
			fields: fields{},
			args: args{
				value: time.Now(),
			},
			wantErr: false,
		},
		{
			name:   "bytes",
			fields: fields{},
			args: args{
				value: []byte("2022-01-27T15:34:52.9368423+08:00"),
			},
			wantErr: false,
		},
		{
			name:   "string",
			fields: fields{},
			args: args{
				value: "2022-01-27T15:34:52.9368423+08:00",
			},
			wantErr: false,
		},
		{
			name:   "error",
			fields: fields{},
			args: args{
				value: 123,
			},
			wantErr: true,
		},
		{
			name:   "nil",
			fields: fields{},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nt := &NullTime{
				Time:  tt.fields.Time,
				Valid: tt.fields.Valid,
			}
			if err := nt.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:20
// @description: test null time type Value()
func TestNullTime_Value(t *testing.T) {
	now := time.Now()
	type fields struct {
		Time  time.Time
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Time:  now,
				Valid: true,
			},
			want:    now,
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Time:  now,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nt := NullTime{
				Time:  tt.fields.Time,
				Valid: tt.fields.Valid,
			}
			got, err := nt.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:24
// @description: test null uint16 type Scan()
func TestNullUInt16_Scan(t *testing.T) {
	type fields struct {
		Inner uint16
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: uint16(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullUInt16{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:48
// @description: test null uint16 type String()
func TestNullUInt16_String(t *testing.T) {
	type fields struct {
		Inner uint16
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullUInt16{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:20
// @description: test null uint16 type Value()
func TestNullUInt16_Value(t *testing.T) {
	type fields struct {
		Inner uint16
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    uint16(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullUInt16{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:24
// @description: test null uint32 type Scan()
func TestNullUInt32_Scan(t *testing.T) {
	type fields struct {
		Inner uint32
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: uint32(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullUInt32{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:48
// @description: test null uint32 type String()
func TestNullUInt32_String(t *testing.T) {
	type fields struct {
		Inner uint32
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullUInt32{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:20
// @description: test null uint32 type Value()
func TestNullUInt32_Value(t *testing.T) {
	type fields struct {
		Inner uint32
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    uint32(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullUInt32{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:24
// @description: test null uint64 type Scan()
func TestNullUInt64_Scan(t *testing.T) {
	type fields struct {
		Inner uint64
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: uint64(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullUInt64{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:48
// @description: test null uint64 type String()
func TestNullUInt64_String(t *testing.T) {
	type fields struct {
		Inner uint64
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullUInt64{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:20
// @description: test null uint64 type Value()
func TestNullUInt64_Value(t *testing.T) {
	type fields struct {
		Inner uint64
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    uint64(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullUInt64{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:25
// @description: test null uint8 type Scan()
func TestNullUInt8_Scan(t *testing.T) {
	type fields struct {
		Inner uint8
		Valid bool
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: uint8(1),
			},
			wantErr: false,
		},
		{
			name: "error",
			fields: fields{
				Inner: 1,
				Valid: true,
			},
			args: args{
				value: 1,
			},
			wantErr: true,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			args: args{
				value: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NullUInt8{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if err := n.Scan(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 16:48
// @description: test null uint8 type String()
func TestNullUInt8_String(t *testing.T) {
	type fields struct {
		Inner uint8
		Valid bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want: "123",
		},
		{
			name: "null",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want: "NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullUInt8{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			if got := n.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:20
// @description: test null uint8 type Value()
func TestNullUInt8_Value(t *testing.T) {
	type fields struct {
		Inner uint8
		Valid bool
	}
	tests := []struct {
		name    string
		fields  fields
		want    driver.Value
		wantErr bool
	}{
		{
			name: "common",
			fields: fields{
				Inner: 123,
				Valid: true,
			},
			want:    uint8(123),
			wantErr: false,
		},
		{
			name: "nil",
			fields: fields{
				Inner: 0,
				Valid: false,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NullUInt8{
				Inner: tt.fields.Inner,
				Valid: tt.fields.Valid,
			}
			got, err := n.Value()
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:20
// @description: test raw message type MarshalJson() interface
func TestRawMessage_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		m       RawMessage
		want    []byte
		wantErr bool
	}{
		{
			name:    "common",
			m:       RawMessage(`{"a":"b"}`),
			want:    []byte(`{"a":"b"}`),
			wantErr: false,
		},
		{
			name:    "nil",
			m:       nil,
			want:    []byte("null"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.m.MarshalJSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MarshalJSON() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/27 17:21
// @description: test raw message type UnmarshalJson() interface
func TestRawMessage_UnmarshalJSON(t *testing.T) {
	common := RawMessage(`{"a":"b"}`)
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		m       *RawMessage
		args    args
		wantErr bool
	}{
		{
			name: "common",
			m:    &common,
			args: args{
				data: []byte(`{"a":"b"}`),
			},
			wantErr: false,
		},
		{
			name:    "error",
			m:       nil,
			args:    args{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.m.UnmarshalJSON(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
