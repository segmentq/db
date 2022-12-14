package db

import (
	"context"
	"fmt"
	api "github.com/segmentq/protos-api-go"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/buntdb"
	"testing"
	"time"
)

func TestDB_GetIndexByName(t *testing.T) {
	d := testNewDB(t)

	indexName := "hello"
	indexDefinition := &api.IndexDefinition{
		Name: indexName,
		Fields: []*api.FieldDefinition{
			{
				Name:      "name",
				DataType:  &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
				IsPrimary: true,
			},
			{
				Name:     "age",
				DataType: &api.FieldDefinition_Geo{Geo: api.GeoType_DATA_TYPE_RANGE},
			},
		},
	}

	var start time.Time
	start = time.Now()

	// Create an index called "hello" with "name" and "age" fields
	createdIndex, _ := d.CreateIndex(indexDefinition)

	fmt.Printf("CreateIndex: %s\n", time.Since(start))
	start = time.Now()

	returnedIndex, _ := d.GetIndexByName(indexName)

	fmt.Printf("GetIndexByName: %s\n", time.Since(start))

	assert.Equal(t, createdIndex, returnedIndex)
}

func TestDB_ListIndexes(t *testing.T) {
	d := testNewDB(t)

	indexSubset := []*api.IndexDefinition{
		{
			Name: "banana",
		},
		{
			Name: "apple",
		},
		{
			Name: "mango",
		},
		{
			Name: "pear",
		},
	}

	for _, index := range indexSubset {
		_, _ = d.CreateIndex(index)
	}

	start := time.Now()
	indexList := d.ListIndexes()
	fmt.Printf("ListIndexes: %s\n", time.Since(start))

	assert.Subset(t, indexList, indexSubset)
}

func TestDB_TruncateIndex(t *testing.T) {
	d := testNewDB(t)

	// Create an index called "hello" with "name" and "age" fields
	index, _ := d.CreateIndex(&api.IndexDefinition{
		Name: "hello",
		Fields: []*api.FieldDefinition{
			{
				Name:      "name",
				DataType:  &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
				IsPrimary: true,
			},
			{
				Name:     "age",
				DataType: &api.FieldDefinition_Geo{Geo: api.GeoType_DATA_TYPE_RANGE},
			},
		},
	})

	// Add a segment to the index
	_, _ = index.InsertSegment(&api.Segment{
		Fields: []*api.SegmentField{
			{
				Name: "name",
				Value: &api.SegmentField_StringValue{
					StringValue: &api.SegmentFieldString{
						Value: "Millennial",
					},
				},
			},
			{
				Name: "age",
				Value: &api.SegmentField_RangeIntValue{
					RangeIntValue: &api.SegmentFieldRangeInt{
						Min: 20,
						Max: 39,
					},
				},
			},
		},
	})

	// Add another segment to the index
	_, _ = index.InsertSegment(&api.Segment{
		Fields: []*api.SegmentField{
			{
				Name: "name",
				Value: &api.SegmentField_StringValue{
					StringValue: &api.SegmentFieldString{
						Value: "OAP",
					},
				},
			},
			{
				Name: "age",
				Value: &api.SegmentField_RangeIntValue{
					RangeIntValue: &api.SegmentFieldRangeInt{
						Min: 65,
						Max: 99,
					},
				},
			},
		},
	})

	// Call Truncate
	assert.NoError(t, d.TruncateIndex("hello"))
}

func TestDB_DeleteIndex(t *testing.T) {
	d := testNewDB(t)

	// Create an index called "hello" with "name" and "age" fields
	index, _ := d.CreateIndex(&api.IndexDefinition{
		Name: "hello",
		Fields: []*api.FieldDefinition{
			{
				Name:      "name",
				DataType:  &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
				IsPrimary: true,
			},
			{
				Name:     "age",
				DataType: &api.FieldDefinition_Geo{Geo: api.GeoType_DATA_TYPE_RANGE},
			},
		},
	})

	// Add a segment to the index
	_, _ = index.InsertSegment(&api.Segment{
		Fields: []*api.SegmentField{
			{
				Name: "name",
				Value: &api.SegmentField_StringValue{
					StringValue: &api.SegmentFieldString{
						Value: "Millennial",
					},
				},
			},
			{
				Name: "age",
				Value: &api.SegmentField_RangeIntValue{
					RangeIntValue: &api.SegmentFieldRangeInt{
						Min: 20,
						Max: 39,
					},
				},
			},
		},
	})

	// Add another segment to the index
	_, _ = index.InsertSegment(&api.Segment{
		Fields: []*api.SegmentField{
			{
				Name: "name",
				Value: &api.SegmentField_StringValue{
					StringValue: &api.SegmentFieldString{
						Value: "OAP",
					},
				},
			},
			{
				Name: "age",
				Value: &api.SegmentField_RangeIntValue{
					RangeIntValue: &api.SegmentFieldRangeInt{
						Min: 65,
						Max: 99,
					},
				},
			},
		},
	})

	// Call Delete
	returnedIndex, err := d.DeleteIndex("hello")
	assert.Equal(t, index, returnedIndex)
	assert.NoError(t, err)
}

func TestDB_CreateIndex(t *testing.T) {
	db := testNewDB(t)
	index1 := &api.IndexDefinition{
		Name: "people",
		Fields: []*api.FieldDefinition{
			{
				Name:      "email",
				DataType:  &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
				IsPrimary: true,
			},
			{
				Name:     "firstname",
				DataType: &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
			},
		},
	}

	dbWithIndex := testNewDB(t)
	_, _ = dbWithIndex.CreateIndex(index1)

	type fields struct {
		ctx    context.Context
		engine *buntdb.DB
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	type args struct {
		indexDefinition *api.IndexDefinition
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Index
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "golden path",
			fields: fields{
				ctx:    db.ctx,
				engine: db.engine,
				idx:    db.idx,
				fields: db.fields,
			},
			args: args{indexDefinition: index1},
			want: &Index{
				db:         db,
				definition: index1,
			},
			wantErr: assert.NoError,
		},
		{
			name: "index already exists",
			fields: fields{
				ctx:    dbWithIndex.ctx,
				engine: dbWithIndex.engine,
				idx:    dbWithIndex.idx,
				fields: dbWithIndex.fields,
			},
			args:    args{indexDefinition: index1},
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
			got, err := db.CreateIndex(tt.args.indexDefinition)
			if !tt.wantErr(t, err, fmt.Sprintf("CreateIndex(%v)", tt.args.indexDefinition)) {
				return
			}
			assert.Equalf(t, tt.want, got, "CreateIndex(%v)", tt.args.indexDefinition)
		})
	}
}

func TestDB_createIndexField(t *testing.T) {
	db := testNewDB(t)

	type fields struct {
		ctx    context.Context
		engine *buntdb.DB
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	type args struct {
		path  string
		field *api.FieldDefinition
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
				ctx:    db.ctx,
				engine: db.engine,
				idx:    db.idx,
				fields: db.fields,
			},
			args: args{
				path: "banana",
				field: &api.FieldDefinition{
					Name:     "man",
					DataType: &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "nil field",
			fields: fields{
				ctx:    db.ctx,
				engine: db.engine,
				idx:    db.idx,
				fields: db.fields,
			},
			args: args{
				path:  "banana",
				field: nil,
			},
			wantErr: assert.Error,
		},
		{
			name: "nil data type for field",
			fields: fields{
				ctx:    db.ctx,
				engine: db.engine,
				idx:    db.idx,
				fields: db.fields,
			},
			args: args{
				path: "banana",
				field: &api.FieldDefinition{
					Name:     "man",
					DataType: nil,
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
			tt.wantErr(t, db.createIndexField(tt.args.path, tt.args.field), fmt.Sprintf("createIndexField(%v, %v)", tt.args.path, tt.args.field))
		})
	}
}

func TestDB_createIndexFields(t *testing.T) {
	db := testNewDB(t)

	type fields struct {
		ctx    context.Context
		engine *buntdb.DB
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	type args struct {
		path   string
		fields []*api.FieldDefinition
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
				ctx:    db.ctx,
				engine: db.engine,
				idx:    db.idx,
				fields: db.fields,
			},
			args: args{
				path: "banana",
				fields: []*api.FieldDefinition{
					{
						Name:     "man",
						DataType: &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "nil field",
			fields: fields{
				ctx:    db.ctx,
				engine: db.engine,
				idx:    db.idx,
				fields: db.fields,
			},
			args: args{
				path:   "banana",
				fields: []*api.FieldDefinition{},
			},
			wantErr: assert.NoError,
		},
		{
			name: "nil data type for field",
			fields: fields{
				ctx:    db.ctx,
				engine: db.engine,
				idx:    db.idx,
				fields: db.fields,
			},
			args: args{
				path: "banana",
				fields: []*api.FieldDefinition{
					{
						Name:     "man",
						DataType: nil,
					},
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
			tt.wantErr(t, db.createIndexFields(tt.args.path, tt.args.fields), fmt.Sprintf("createIndexFields(%v, %v)", tt.args.path, tt.args.fields))
		})
	}
}

func TestDB_loadIndexFields(t *testing.T) {
	db1 := testNewDB(t)
	index1 := &api.IndexDefinition{
		Name: "banana",
		Fields: []*api.FieldDefinition{
			{
				Name:     "man",
				DataType: &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
			},
		},
	}

	type fields struct {
		ctx    context.Context
		engine *buntdb.DB
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	type args struct {
		index *api.IndexDefinition
	}
	type sets struct {
		idx    map[string]*api.IndexDefinition
		fields map[string]map[string]*api.FieldDefinition
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		sets   sets
	}{
		{
			name: "golden path",
			fields: fields{
				ctx:    db1.ctx,
				engine: db1.engine,
				idx:    db1.idx,
				fields: db1.fields,
			},
			args: args{index: index1},
			sets: sets{
				idx: map[string]*api.IndexDefinition{
					index1.Name: index1,
				},
				fields: map[string]map[string]*api.FieldDefinition{
					index1.Name: {
						index1.Fields[0].Name: index1.Fields[0],
					},
				},
			},
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
			db.loadIndexFields(tt.args.index)

			if tt.sets.idx != nil {
				assert.Equalf(t, tt.sets.idx, db.idx, fmt.Sprintf("loadIndexFields(%v)", tt.args.index))
			}

			if tt.sets.fields != nil {
				assert.Equalf(t, tt.sets.fields, db.fields, fmt.Sprintf("loadIndexFields(%v)", tt.args.index))
			}
		})
	}
}

func TestIndex_UnmarshallPrimaryValue(t *testing.T) {
	d := testNewDB(t)
	stringDef := &api.IndexDefinition{
		Name: "fruits",
		Fields: []*api.FieldDefinition{
			{
				Name:      "name",
				DataType:  &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_STRING},
				IsPrimary: true,
			},
		},
	}
	intDef := &api.IndexDefinition{
		Name: "fruits",
		Fields: []*api.FieldDefinition{
			{
				Name:      "length",
				DataType:  &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_INT},
				IsPrimary: true,
			},
		},
	}
	floatDef := &api.IndexDefinition{
		Name: "fruits",
		Fields: []*api.FieldDefinition{
			{
				Name:      "weight",
				DataType:  &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_FLOAT},
				IsPrimary: true,
			},
		},
	}
	boolDef := &api.IndexDefinition{
		Name: "fruits",
		Fields: []*api.FieldDefinition{
			{
				Name:      "isBruised",
				DataType:  &api.FieldDefinition_Scalar{Scalar: api.ScalarType_DATA_TYPE_BOOL},
				IsPrimary: true,
			},
		},
	}

	type fields struct {
		db         *DB
		definition *api.IndexDefinition
	}
	type args struct {
		value string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *api.SegmentField
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:   "golden path string",
			fields: fields{db: d, definition: stringDef},
			args:   args{value: "banana"},
			want: &api.SegmentField{
				Name: "name",
				Value: &api.SegmentField_StringValue{
					StringValue: &api.SegmentFieldString{
						Value: "banana",
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name:   "golden path int",
			fields: fields{db: d, definition: intDef},
			args:   args{value: "123"},
			want: &api.SegmentField{
				Name: "length",
				Value: &api.SegmentField_IntValue{
					IntValue: &api.SegmentFieldInt{
						Value: 123,
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name:   "golden path float",
			fields: fields{db: d, definition: floatDef},
			args:   args{value: "1.2345"},
			want: &api.SegmentField{
				Name: "weight",
				Value: &api.SegmentField_FloatValue{
					FloatValue: &api.SegmentFieldFloat{
						Value: 1.2345,
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name:   "golden path bool",
			fields: fields{db: d, definition: boolDef},
			args:   args{value: "true"},
			want: &api.SegmentField{
				Name: "isBruised",
				Value: &api.SegmentField_BoolValue{
					BoolValue: &api.SegmentFieldBool{
						Value: true,
					},
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
			got, err := i.UnmarshallPrimaryValue(tt.args.value)
			if !tt.wantErr(t, err, fmt.Sprintf("UnmarshallPrimaryValue(%v)", tt.args.value)) {
				return
			}
			assert.Equalf(t, tt.want, got, "UnmarshallPrimaryValue(%v)", tt.args.value)
		})
	}
}
