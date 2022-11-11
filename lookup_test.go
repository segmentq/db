package db

import (
	"context"
	"fmt"
	api "github.com/segmentq/protos-api-go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
	"testing"
	"time"
)

func TestDB_Lookup(t *testing.T) {
	d, _ := NewDB(context.Background())

	var start time.Time
	start = time.Now()

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

	fmt.Printf("CreateIndex: %s\n", time.Since(start))
	start = time.Now()

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

	fmt.Printf("InsertSegment1: %s\n", time.Since(start))
	start = time.Now()

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

	fmt.Printf("InsertSegment2: %s\n", time.Since(start))
	start = time.Now()

	// Lookup a "Millennial"
	it, _ := index.Lookup(&api.Lookup{
		Fields: []*api.LookupField{
			{
				Name: "name",
				Value: &api.LookupField_StringValue{
					StringValue: &api.SegmentFieldString{
						Value: "Millennial",
					},
				},
			},
			{
				Name: "age",
				Value: &api.LookupField_RangeIntValue{
					RangeIntValue: &api.SegmentFieldRangeInt{
						Min: 21,
						Max: 21,
					},
				},
			},
		},
	})

	fmt.Printf("Lookup2Fields: %s\n", time.Since(start))

	for {
		key, err := it.Next(nil)
		if err == iterator.Done {
			break
		}

		assert.NoError(t, err)
		assert.Equal(t, "Millennial", key)
	}

	start = time.Now()

	// Lookup an "OAP"
	it, _ = index.Lookup(&api.Lookup{
		Fields: []*api.LookupField{
			{
				Name: "age",
				Value: &api.LookupField_RangeIntValue{
					RangeIntValue: &api.SegmentFieldRangeInt{
						Min: 66,
						Max: 66,
					},
				},
			},
		},
	})

	fmt.Printf("Lookup1Fields: %s\n", time.Since(start))

	for {
		key, err := it.Next(nil)
		if err == iterator.Done {
			break
		}

		assert.NoError(t, err)
		assert.Equal(t, "OAP", key)
	}

	// Lookup both
	it, _ = index.Lookup(&api.Lookup{
		Fields: []*api.LookupField{
			{
				Name: "age",
				Value: &api.LookupField_RangeIntValue{
					RangeIntValue: &api.SegmentFieldRangeInt{
						Min: 20,
						Max: 66,
					},
				},
			},
		},
	})

	collector := make([]string, 0)
	for {
		key, err := it.Next(nil)
		if err == iterator.Done {
			break
		}

		assert.NoError(t, err)

		collector = append(collector, key)
	}

	assert.Equal(t, len(collector), 2)
	assert.Equal(t, "Millennial", collector[0])
	assert.Equal(t, "OAP", collector[1])
}
