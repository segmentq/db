package db

import (
	"context"
	"fmt"
	api "github.com/segmentq/protos-api-go"
	"github.com/stretchr/testify/assert"
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
