package db

import (
	"context"
	"fmt"
	api "github.com/segmentq/protos-api-go"
	"google.golang.org/api/iterator"
	"testing"
)

func TestDB_CreateIndex(t *testing.T) {
	d, _ := NewDB(context.Background())

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

	for {
		key, err := it.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}
		if key != "Millennial" {
			panic(fmt.Sprint("received", key))
		}
	}
}
