package db

import (
	"context"
	"fmt"
	api "github.com/segmentq/protos-api-go"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/buntdb"
	"testing"
)

func testSingleFieldIndex(t *testing.T, db *DB, name string) *Index {
	index, err := db.CreateIndex(getSingleFieldIndex(name))

	if err != nil {
		t.Fatal(err)
	}

	return index
}

func getSingleFieldIndex(name string) *api.IndexDefinition {
	return &api.IndexDefinition{
		Name: name,
		Fields: []*api.FieldDefinition{
			{
				Name:      "name",
				DataType:  &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
				IsPrimary: true,
			},
		},
	}
}

func testSingleFieldIndexSegment(t *testing.T, db *DB, indexName string, segmentKey string) *Segment {
	index := testSingleFieldIndex(t, db, indexName)

	segment, err := index.InsertSegment(getSingleFieldSegment(segmentKey))

	if err != nil {
		t.Fatal(err)
	}

	return segment
}

func getSingleFieldSegment(key string) *api.Segment {
	return &api.Segment{
		Fields: []*api.SegmentField{
			{
				Name: "name",
				Value: &api.SegmentField_StringValue{
					StringValue: &api.SegmentFieldString{
						Value: key,
					},
				},
			},
		},
	}
}

func TestDB_DeleteSegment(t *testing.T) {
	d := testNewDB(t)

	indexName := "demographic"
	segmentKey := "Millennial"

	segment := testSingleFieldIndexSegment(t, d, indexName, segmentKey)

	type fields struct {
		ctx    context.Context
		engine *buntdb.DB
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	type args struct {
		indexName  string
		segmentKey string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "golden path",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName:  indexName,
				segmentKey: segmentKey,
			},
			want:    segment,
			wantErr: assert.NoError,
		},
		{
			name: "index not found",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName:  "banana",
				segmentKey: segmentKey,
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "segment not found",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName:  indexName,
				segmentKey: "banana",
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				ctx:    tt.fields.ctx,
				engine: tt.fields.engine,
				idx:    tt.fields.idx,
				fields: tt.fields.fields,
			}
			got, err := db.DeleteSegment(tt.args.indexName, tt.args.segmentKey)
			if !tt.wantErr(t, err, fmt.Sprintf("DeleteSegment(%v, %v)", tt.args.indexName, tt.args.segmentKey)) {
				return
			}
			assert.Equalf(t, tt.want, got, "DeleteSegment(%v, %v)", tt.args.indexName, tt.args.segmentKey)

			if got == nil {
				return
			}

			got2, err2 := db.GetSegmentByKey(tt.args.indexName, tt.args.segmentKey)
			assert.Nilf(t, got2, "DeleteSegment(%v, %v)", tt.args.indexName, tt.args.segmentKey)
			assert.ErrorIs(t, err2, ErrSegmentNotFound, "DeleteSegment(%v, %v)", tt.args.indexName, tt.args.segmentKey)
		})
	}
}

func TestDB_GetSegmentByKey(t *testing.T) {
	d := testNewDB(t)

	indexName := "demographic"
	segmentKey := "Millennial"

	segment := testSingleFieldIndexSegment(t, d, indexName, segmentKey)

	type fields struct {
		ctx    context.Context
		engine *buntdb.DB
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	type args struct {
		indexName  string
		segmentKey string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "golden path",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName:  indexName,
				segmentKey: segmentKey,
			},
			want:    segment,
			wantErr: assert.NoError,
		},
		{
			name: "index not found",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName:  "banana",
				segmentKey: segmentKey,
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "segment not found",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName:  indexName,
				segmentKey: "banana",
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				ctx:    tt.fields.ctx,
				engine: tt.fields.engine,
				idx:    tt.fields.idx,
				fields: tt.fields.fields,
			}
			got, err := db.GetSegmentByKey(tt.args.indexName, tt.args.segmentKey)
			if !tt.wantErr(t, err, fmt.Sprintf("GetSegmentByKey(%v, %v)", tt.args.indexName, tt.args.segmentKey)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetSegmentByKey(%v, %v)", tt.args.indexName, tt.args.segmentKey)
		})
	}
}

func TestDB_InsertSegment(t *testing.T) {
	d := testNewDB(t)

	indexName := "demographic"
	segmentKey := "Millennial"

	index := testSingleFieldIndex(t, d, indexName)
	segment := getSingleFieldSegment(segmentKey)

	type fields struct {
		ctx    context.Context
		engine *buntdb.DB
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	type args struct {
		indexName string
		segment   *api.Segment
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "golden path",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName: indexName,
				segment:   segment,
			},
			want: &Segment{
				db:      d,
				index:   index,
				segment: segment,
			},
			wantErr: assert.NoError,
		},
		{
			name: "index not found",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName: "banana",
				segment:   segment,
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				ctx:    tt.fields.ctx,
				engine: tt.fields.engine,
				idx:    tt.fields.idx,
				fields: tt.fields.fields,
			}
			got, err := db.InsertSegment(tt.args.indexName, tt.args.segment)
			if !tt.wantErr(t, err, fmt.Sprintf("InsertSegment(%v, %v)", tt.args.indexName, tt.args.segment)) {
				return
			}
			assert.Equalf(t, tt.want, got, "InsertSegment(%v, %v)", tt.args.indexName, tt.args.segment)
		})
	}
}

func TestDB_ReplaceSegment(t *testing.T) {
	d := testNewDB(t)

	indexName := "demographic"
	segmentKey := "Millennial"
	newSegmentKey := "Boomer"

	segment := testSingleFieldIndexSegment(t, d, indexName, segmentKey)
	newSeg := getSingleFieldSegment(newSegmentKey)

	type fields struct {
		ctx    context.Context
		engine *buntdb.DB
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	type args struct {
		indexName  string
		segmentKey string
		newSegment *api.Segment
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "golden path",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName:  indexName,
				segmentKey: segmentKey,
				newSegment: newSeg,
			},
			want: &Segment{
				db:      d,
				index:   segment.index,
				segment: newSeg,
			},
			wantErr: assert.NoError,
		},
		{
			name: "index not found",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName:  "banana",
				segmentKey: segmentKey,
				newSegment: newSeg,
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "segment not found",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName:  indexName,
				segmentKey: "non-existent",
				newSegment: newSeg,
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				ctx:    tt.fields.ctx,
				engine: tt.fields.engine,
				idx:    tt.fields.idx,
				fields: tt.fields.fields,
			}
			got, err := db.ReplaceSegment(tt.args.indexName, tt.args.segmentKey, tt.args.newSegment)
			if !tt.wantErr(t, err, fmt.Sprintf("ReplaceSegment(%v, %v, %v)", tt.args.indexName, tt.args.segmentKey, tt.args.newSegment)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ReplaceSegment(%v, %v, %v)", tt.args.indexName, tt.args.segmentKey, tt.args.newSegment)
		})
	}
}

func TestIndex_DeleteSegment(t *testing.T) {
	d := testNewDB(t)

	indexName := "demographic"
	segmentKey := "Millennial"

	segment := testSingleFieldIndexSegment(t, d, indexName, segmentKey)

	type fields struct {
		db         *DB
		definition *api.IndexDefinition
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "golden path",
			fields: fields{
				db:         d,
				definition: segment.index.definition,
			},
			args: args{
				key: segmentKey,
			},
			want:    segment,
			wantErr: assert.NoError,
		},
		{
			name: "segment not found",
			fields: fields{
				db:         d,
				definition: segment.index.definition,
			},
			args: args{
				key: "banana",
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Index{
				db:         tt.fields.db,
				definition: tt.fields.definition,
			}
			got, err := i.DeleteSegment(tt.args.key)
			if !tt.wantErr(t, err, fmt.Sprintf("DeleteSegment(%v)", tt.args.key)) {
				return
			}
			assert.Equalf(t, tt.want, got, "DeleteSegment(%v)", tt.args.key)
		})
	}
}

func TestIndex_GetSegmentByKey(t *testing.T) {
	d := testNewDB(t)

	indexName := "demographic"
	segmentKey := "Millennial"

	segment := testSingleFieldIndexSegment(t, d, indexName, segmentKey)

	type fields struct {
		db         *DB
		definition *api.IndexDefinition
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "golden path",
			fields: fields{
				db:         d,
				definition: segment.index.definition,
			},
			args: args{
				key: segmentKey,
			},
			want:    segment,
			wantErr: assert.NoError,
		},
		{
			name: "segment not found",
			fields: fields{
				db:         d,
				definition: segment.index.definition,
			},
			args: args{
				key: "banana",
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Index{
				db:         tt.fields.db,
				definition: tt.fields.definition,
			}
			got, err := i.GetSegmentByKey(tt.args.key)
			if !tt.wantErr(t, err, fmt.Sprintf("GetSegmentByKey(%v)", tt.args.key)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetSegmentByKey(%v)", tt.args.key)
		})
	}
}

func TestIndex_InsertSegment(t *testing.T) {
	type fields struct {
		db         *DB
		definition *api.IndexDefinition
	}
	type args struct {
		segment *api.Segment
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Index{
				db:         tt.fields.db,
				definition: tt.fields.definition,
			}
			got, err := i.InsertSegment(tt.args.segment)
			if !tt.wantErr(t, err, fmt.Sprintf("InsertSegment(%v)", tt.args.segment)) {
				return
			}
			assert.Equalf(t, tt.want, got, "InsertSegment(%v)", tt.args.segment)
		})
	}
}

func TestIndex_ReplaceSegment(t *testing.T) {
	type fields struct {
		db         *DB
		definition *api.IndexDefinition
	}
	type args struct {
		key        string
		newSegment *api.Segment
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Index{
				db:         tt.fields.db,
				definition: tt.fields.definition,
			}
			got, err := i.ReplaceSegment(tt.args.key, tt.args.newSegment)
			if !tt.wantErr(t, err, fmt.Sprintf("ReplaceSegment(%v, %v)", tt.args.key, tt.args.newSegment)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ReplaceSegment(%v, %v)", tt.args.key, tt.args.newSegment)
		})
	}
}

func TestNewLookupStringer(t *testing.T) {
	type args struct {
		field *api.LookupField
		iter  func(key, value string) bool
	}
	tests := []struct {
		name string
		args args
		want *Stringer
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewLookupStringer(tt.args.field, tt.args.iter), "NewLookupStringer(%v, %v)", tt.args.field, tt.args.iter)
		})
	}
}

func TestNewSegmentStringer(t *testing.T) {
	type args struct {
		field *api.SegmentField
		iter  func(key, value string) bool
	}
	tests := []struct {
		name string
		args args
		want *Stringer
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewSegmentStringer(tt.args.field, tt.args.iter), "NewSegmentStringer(%v, %v)", tt.args.field, tt.args.iter)
		})
	}
}

func TestSegment_Delete(t *testing.T) {
	type fields struct {
		db      *DB
		index   *Index
		segment *api.Segment
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Segment{
				db:      tt.fields.db,
				index:   tt.fields.index,
				segment: tt.fields.segment,
			}
			tt.wantErr(t, s.Delete(), fmt.Sprintf("Delete()"))
		})
	}
}

func TestSegment_Insert(t *testing.T) {
	type fields struct {
		db      *DB
		index   *Index
		segment *api.Segment
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Segment{
				db:      tt.fields.db,
				index:   tt.fields.index,
				segment: tt.fields.segment,
			}
			tt.wantErr(t, s.Insert(), fmt.Sprintf("Insert()"))
		})
	}
}

func TestSegment_Replace(t *testing.T) {
	type fields struct {
		db      *DB
		index   *Index
		segment *api.Segment
	}
	type args struct {
		new *api.Segment
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Segment{
				db:      tt.fields.db,
				index:   tt.fields.index,
				segment: tt.fields.segment,
			}
			got, err := s.Replace(tt.args.new)
			if !tt.wantErr(t, err, fmt.Sprintf("Replace(%v)", tt.args.new)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Replace(%v)", tt.args.new)
		})
	}
}

func TestSegment_deleteFromIndexName(t *testing.T) {
	type fields struct {
		db      *DB
		index   *Index
		segment *api.Segment
	}
	type args struct {
		indexName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Segment{
				db:      tt.fields.db,
				index:   tt.fields.index,
				segment: tt.fields.segment,
			}
			tt.wantErr(t, s.deleteFromIndexName(tt.args.indexName), fmt.Sprintf("deleteFromIndexName(%v)", tt.args.indexName))
		})
	}
}

func TestSegment_generateIndexMap(t *testing.T) {
	type fields struct {
		db      *DB
		index   *Index
		segment *api.Segment
	}
	type args struct {
		indexName string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantPrimary string
		wantInserts map[string]map[string]string
		wantErr     assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Segment{
				db:      tt.fields.db,
				index:   tt.fields.index,
				segment: tt.fields.segment,
			}
			gotPrimary, gotInserts, err := s.generateIndexMap(tt.args.indexName)
			if !tt.wantErr(t, err, fmt.Sprintf("generateIndexMap(%v)", tt.args.indexName)) {
				return
			}
			assert.Equalf(t, tt.wantPrimary, gotPrimary, "generateIndexMap(%v)", tt.args.indexName)
			assert.Equalf(t, tt.wantInserts, gotInserts, "generateIndexMap(%v)", tt.args.indexName)
		})
	}
}

func TestSegment_insertToIndexName(t *testing.T) {
	type fields struct {
		db      *DB
		index   *Index
		segment *api.Segment
	}
	type args struct {
		indexName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Segment{
				db:      tt.fields.db,
				index:   tt.fields.index,
				segment: tt.fields.segment,
			}
			tt.wantErr(t, s.insertToIndexName(tt.args.indexName), fmt.Sprintf("insertToIndexName(%v)", tt.args.indexName))
		})
	}
}

func TestSegment_replaceInIndexName(t *testing.T) {
	type fields struct {
		db      *DB
		index   *Index
		segment *api.Segment
	}
	type args struct {
		indexName string
		new       *api.Segment
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Segment
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Segment{
				db:      tt.fields.db,
				index:   tt.fields.index,
				segment: tt.fields.segment,
			}
			got, err := s.replaceInIndexName(tt.args.indexName, tt.args.new)
			if !tt.wantErr(t, err, fmt.Sprintf("replaceInIndexName(%v, %v)", tt.args.indexName, tt.args.new)) {
				return
			}
			assert.Equalf(t, tt.want, got, "replaceInIndexName(%v, %v)", tt.args.indexName, tt.args.new)
		})
	}
}

func TestStringer_Marshall(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			tt.wantErr(t, s.MarshallText(), fmt.Sprintf("MarshallText()"))
		})
	}
}

func TestStringer_fromBlobValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromBlobValue(tt.args.key, tt.args.value), "fromBlobValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_fromBoolValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromBoolValue(tt.args.key, tt.args.value), "fromBoolValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_fromFloatValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromFloatValue(tt.args.key, tt.args.value), "fromFloatValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_fromGeoPointValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value *api.SegmentFieldGeoPoint
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromGeoPointValue(tt.args.key, tt.args.value), "fromGeoPointValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_fromGeoRectValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value *api.SegmentFieldGeoRect
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromGeoRectValue(tt.args.key, tt.args.value), "fromGeoRectValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_fromIntValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromIntValue(tt.args.key, tt.args.value), "fromIntValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_fromRangeFloatValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value *api.SegmentFieldRangeFloat
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromRangeFloatValue(tt.args.key, tt.args.value), "fromRangeFloatValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_fromRangeIntValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value *api.SegmentFieldRangeInt
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromRangeIntValue(tt.args.key, tt.args.value), "fromRangeIntValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_fromRepeatedBlobValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedBlobValue(tt.args.values)
		})
	}
}

func TestStringer_fromRepeatedBoolValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedBoolValue(tt.args.values)
		})
	}
}

func TestStringer_fromRepeatedFloatValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedFloatValue(tt.args.values)
		})
	}
}

func TestStringer_fromRepeatedGeoPointValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []*api.SegmentFieldGeoPoint
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedGeoPointValue(tt.args.values)
		})
	}
}

func TestStringer_fromRepeatedGeoRectValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []*api.SegmentFieldGeoRect
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedGeoRectValue(tt.args.values)
		})
	}
}

func TestStringer_fromRepeatedIntValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedIntValue(tt.args.values)
		})
	}
}

func TestStringer_fromRepeatedRangeFloatValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []*api.SegmentFieldRangeFloat
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedRangeFloatValue(tt.args.values)
		})
	}
}

func TestStringer_fromRepeatedRangeIntValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []*api.SegmentFieldRangeInt
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedRangeIntValue(tt.args.values)
		})
	}
}

func TestStringer_fromRepeatedStringValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedStringValue(tt.args.values)
		})
	}
}

func TestStringer_fromRepeatedUintValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		values []uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			s.fromRepeatedUintValue(tt.args.values)
		})
	}
}

func TestStringer_fromStringValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromStringValue(tt.args.key, tt.args.value), "fromStringValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_fromUintValue(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	type args struct {
		key   int
		value uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			assert.Equalf(t, tt.want, s.fromUintValue(tt.args.key, tt.args.value), "fromUintValue(%v, %v)", tt.args.key, tt.args.value)
		})
	}
}

func TestStringer_marshallLookup(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			tt.wantErr(t, s.marshallLookup(), fmt.Sprintf("marshallLookup()"))
		})
	}
}

func TestStringer_marshallSegment(t *testing.T) {
	type fields struct {
		segmentField *api.SegmentField
		lookupField  *api.LookupField
		iter         func(key, value string) bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stringer{
				segmentField: tt.fields.segmentField,
				lookupField:  tt.fields.lookupField,
				iter:         tt.fields.iter,
			}
			tt.wantErr(t, s.marshallSegment(), fmt.Sprintf("marshallSegment()"))
		})
	}
}

func Test_newSegment(t *testing.T) {
	type args struct {
		db      *DB
		index   *Index
		segment *api.Segment
	}
	tests := []struct {
		name string
		args args
		want *Segment
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, newSegment(tt.args.db, tt.args.index, tt.args.segment), "newSegment(%v, %v, %v)", tt.args.db, tt.args.index, tt.args.segment)
		})
	}
}

func TestIndex_GetAllSegments(t *testing.T) {
	d := testNewDB(t)
	iDef := getSingleFieldIndex("fruits")
	index, _ := d.CreateIndex(iDef)

	banana := getSingleFieldSegment("banana")
	mango := getSingleFieldSegment("mango")
	apple := getSingleFieldSegment("apple")

	_, _ = index.InsertSegment(banana)
	_, _ = index.InsertSegment(mango)
	_, _ = index.InsertSegment(apple)

	type fields struct {
		db         *DB
		definition *api.IndexDefinition
	}
	type args struct {
		iter func(segment *api.Segment) bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "golden path",
			fields: fields{
				db:         d,
				definition: iDef,
			},
			args: args{
				iter: func(segment *api.Segment) bool {
					return segment == banana || segment == mango || segment == apple
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "golden path false in function",
			fields: fields{
				db:         d,
				definition: iDef,
			},
			args: args{
				iter: func(segment *api.Segment) bool {
					return false
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Index{
				db:         tt.fields.db,
				definition: tt.fields.definition,
			}
			tt.wantErr(t, i.GetAllSegments(tt.args.iter), fmt.Sprintf("GetAllSegments(%p)", tt.args.iter))
		})
	}
}

func TestDB_GetAllSegments(t *testing.T) {
	d := testNewDB(t)
	iDef := getSingleFieldIndex("fruits")
	index, _ := d.CreateIndex(iDef)

	banana := getSingleFieldSegment("banana")
	mango := getSingleFieldSegment("mango")
	apple := getSingleFieldSegment("apple")

	_, _ = index.InsertSegment(banana)
	_, _ = index.InsertSegment(mango)
	_, _ = index.InsertSegment(apple)

	type fields struct {
		ctx    context.Context
		engine *buntdb.DB
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	type args struct {
		indexName string
		iter      func(segment *api.Segment) bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "golden path",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName: iDef.Name,
				iter: func(segment *api.Segment) bool {
					return segment == banana || segment == mango || segment == apple
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "golden path false in function",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName: iDef.Name,
				iter: func(segment *api.Segment) bool {
					return false
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "no index",
			fields: fields{
				ctx:    d.ctx,
				engine: d.engine,
				idx:    d.idx,
				fields: d.fields,
			},
			args: args{
				indexName: "banana",
				iter: func(segment *api.Segment) bool {
					return false
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				ctx:    tt.fields.ctx,
				engine: tt.fields.engine,
				idx:    tt.fields.idx,
				fields: tt.fields.fields,
			}
			tt.wantErr(t, db.GetAllSegments(tt.args.indexName, tt.args.iter), fmt.Sprintf("GetAllSegments(%v, %p)", tt.args.indexName, tt.args.iter))
		})
	}
}
