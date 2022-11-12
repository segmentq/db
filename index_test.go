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
	d, _ := NewDB(context.Background())

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
	d, _ := NewDB(context.Background())

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
	d, _ := NewDB(context.Background())

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
	d, _ := NewDB(context.Background())

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
	db, _ := NewDB(context.Background())
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

	dbWithIndex, _ := NewDB(context.Background())
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
	db, _ := NewDB(context.Background())

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
	db, _ := NewDB(context.Background())

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
	db1, _ := NewDB(context.Background())
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
