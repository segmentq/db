package db

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func testNewDB(t *testing.T) *DB {
	db, err := NewDB(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestKey_FieldNameAtIndex(t *testing.T) {
	type fields struct {
		parts     []string
		separator string
	}
	type args struct {
		fieldIndex int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
		want1  bool
	}{
		{
			name: "single field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "primary", "fieldIndex"},
				separator: ":",
			},
			args:  args{fieldIndex: 0},
			want:  "field1",
			want1: true,
		},
		{
			name: "multi field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "field2", "field3", "primary", "fieldIndex"},
				separator: ":",
			},
			args:  args{fieldIndex: 2},
			want:  "field3",
			want1: true,
		},
		{
			name: "field out of range",
			fields: fields{
				parts:     []string{"index", "field1", "primary", "fieldIndex"},
				separator: ":",
			},
			args:  args{fieldIndex: 1},
			want:  "",
			want1: false,
		},
		{
			name: "not enough fields",
			fields: fields{
				parts:     []string{},
				separator: ":",
			},
			args:  args{fieldIndex: 1},
			want:  "",
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Key{
				parts:     tt.fields.parts,
				separator: tt.fields.separator,
			}
			got, got1 := k.FieldNameAtIndex(tt.args.fieldIndex)
			assert.Equalf(t, tt.want, got, "FieldNameAtIndex(%v)", tt.args.fieldIndex)
			assert.Equalf(t, tt.want1, got1, "FieldNameAtIndex(%v)", tt.args.fieldIndex)
		})
	}
}

func TestKey_FieldValueIndex(t *testing.T) {
	type fields struct {
		parts     []string
		separator string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  bool
	}{
		{
			name: "single field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "primary", "fieldIndex"},
				separator: ":",
			},
			want:  "fieldIndex",
			want1: true,
		},
		{
			name: "multi field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "field2", "field3", "primary", "fieldIndex"},
				separator: ":",
			},
			want:  "fieldIndex",
			want1: true,
		},
		{
			name: "not enough fields",
			fields: fields{
				parts:     []string{"index", "field2", "primary"},
				separator: ":",
			},
			want:  "",
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Key{
				parts:     tt.fields.parts,
				separator: tt.fields.separator,
			}
			got, got1 := k.FieldValueIndex()
			assert.Equalf(t, tt.want, got, "FieldValueIndex()")
			assert.Equalf(t, tt.want1, got1, "FieldValueIndex()")
		})
	}
}

func TestKey_IndexId(t *testing.T) {
	type fields struct {
		parts     []string
		separator string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  bool
	}{
		{
			name: "single field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "primary", "fieldIndex"},
				separator: ":",
			},
			want:  "index",
			want1: true,
		},
		{
			name: "multi field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "field2", "field3", "primary", "fieldIndex"},
				separator: ":",
			},
			want:  "index",
			want1: true,
		},
		{
			name: "not enough fields",
			fields: fields{
				parts:     []string{"index", "field2", "primary"},
				separator: ":",
			},
			want:  "",
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Key{
				parts:     tt.fields.parts,
				separator: tt.fields.separator,
			}
			got, got1 := k.IndexId()
			assert.Equalf(t, tt.want, got, "IndexId()")
			assert.Equalf(t, tt.want1, got1, "IndexId()")
		})
	}
}

func TestKey_SegmentKey(t *testing.T) {
	type fields struct {
		parts     []string
		separator string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  bool
	}{
		{
			name: "single field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "primary", "fieldIndex"},
				separator: ":",
			},
			want:  "primary",
			want1: true,
		},
		{
			name: "multi field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "field2", "field3", "primary", "fieldIndex"},
				separator: ":",
			},
			want:  "primary",
			want1: true,
		},
		{
			name: "not enough fields",
			fields: fields{
				parts:     []string{"index", "field2", "primary"},
				separator: ":",
			},
			want:  "",
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Key{
				parts:     tt.fields.parts,
				separator: tt.fields.separator,
			}
			got, got1 := k.SegmentKey()
			assert.Equalf(t, tt.want, got, "SegmentKey()")
			assert.Equalf(t, tt.want1, got1, "SegmentKey()")
		})
	}
}

func TestKey_String(t *testing.T) {
	type fields struct {
		parts     []string
		separator string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "single field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "primary", "fieldIndex"},
				separator: ":",
			},
			want: "index:field1:primary:fieldIndex",
		},
		{
			name: "multi field, golden path",
			fields: fields{
				parts:     []string{"index", "field1", "field2", "field3", "primary", "fieldIndex"},
				separator: ":",
			},
			want: "index:field1:field2:field3:primary:fieldIndex",
		},
		{
			name: "multi field, different separator",
			fields: fields{
				parts:     []string{"index", "field1", "field2", "field3", "primary", "fieldIndex"},
				separator: "£$%^",
			},
			want: "index£$%^field1£$%^field2£$%^field3£$%^primary£$%^fieldIndex",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Key{
				parts:     tt.fields.parts,
				separator: tt.fields.separator,
			}
			assert.Equalf(t, tt.want, k.String(), "String()")
		})
	}
}

func TestKey_fromString(t *testing.T) {
	type fields struct {
		parts     []string
		separator string
	}
	type args struct {
		str string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
		want1  bool
	}{
		{
			name: "single field, golden path",
			fields: fields{
				separator: ":",
			},
			args: args{
				str: "index:field1:primary:fieldIndex",
			},
			want:  "primary",
			want1: true,
		},
		{
			name: "multi field, golden path",
			fields: fields{
				separator: ":",
			},
			args: args{
				str: "index:field1:field2:field3:primary:fieldIndex",
			},
			want:  "primary",
			want1: true,
		},
		{
			name: "multi field, different separator",
			fields: fields{
				separator: "£$%^",
			},
			args: args{
				str: "index£$%^field1£$%^field2£$%^field3£$%^primary£$%^fieldIndex",
			},
			want:  "primary",
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Key{
				parts:     tt.fields.parts,
				separator: tt.fields.separator,
			}
			k.fromString(tt.args.str)
			got, got1 := k.SegmentKey()
			assert.Equalf(t, tt.want, got, "SegmentKey()")
			assert.Equalf(t, tt.want1, got1, "SegmentKey()")
		})
	}
}

func Test_idxKey(t *testing.T) {
	type args struct {
		str []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "golden path",
			args: args{str: []string{"some", "strings", "joined", "together"}},
			want: "some:strings:joined:together",
		},
		{
			name: "empty string",
			args: args{str: []string{}},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, idxKey(tt.args.str...), "idxKey(%v)", tt.args.str)
		})
	}
}

func Test_keyFromString(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		args args
		want Key
	}{
		{
			name: "single field, golden path",
			args: args{
				key: "index:field1:primary:fieldIndex",
			},
			want: Key{
				parts:     []string{"index", "field1", "primary", "fieldIndex"},
				separator: ":",
			},
		},
		{
			name: "multi field, golden path",
			args: args{
				key: "index:field1:field2:field3:primary:fieldIndex",
			},
			want: Key{
				parts:     []string{"index", "field1", "field2", "field3", "primary", "fieldIndex"},
				separator: ":",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, keyFromString(tt.args.key), "keyFromString(%v)", tt.args.key)
		})
	}
}
