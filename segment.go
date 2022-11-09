package db

import (
	"github.com/golang/protobuf/proto"
	api "github.com/segmentq/protos-api-go"
	"github.com/tidwall/buntdb"
	"strconv"
)

type Segment struct {
	db      *DB
	index   *Index
	segment *api.Segment
}

func (db *DB) InsertSegment(indexName string, segment *api.Segment) (*Segment, error) {
	s := newSegment(db, nil, segment)
	return s, s.InsertToIndexName(indexName)
}

func (i *Index) InsertSegment(segment *api.Segment) (*Segment, error) {
	s := newSegment(i.db, i, segment)
	return s, s.Insert()
}

func newSegment(db *DB, index *Index, segment *api.Segment) *Segment {
	return &Segment{
		db:      db,
		index:   index,
		segment: segment,
	}
}

func (s *Segment) Insert() error {
	return s.InsertToIndexName(s.index.definition.Name)
}

func (s *Segment) InsertToIndexName(indexName string) error {
	return s.db.engine.Update(func(tx *buntdb.Tx) error {
		// Find the integer index of the index
		idx, err := tx.Get(idxKey(idxById, indexName), true)
		if err != nil {
			return ErrInternalDBError
		}

		var primary string
		inserts := make(map[string]map[string]string, 0) // map of values by key prefix

		// Gather the values by field name and key in a 0 based map
		for _, field := range s.segment.Fields {
			definition, exists := s.db.fields[indexName][field.Name]
			if !exists {
				if _, ok := s.db.fields[indexName]; !ok {
					return ErrIndexUnknown
				}
				return ErrFieldUnknown
			}

			// Note the primary key, so we can extract the correct value for the key name
			if definition.IsPrimary {
				primary = field.Name
			}

			keyMap := make(map[string]string, 0)

			if err = stringsFromSegmentField(field, func(key string, value string) bool {
				keyMap[key] = value
				return true
			}); err != nil {
				return ErrInternalDBError
			}

			inserts[field.Name] = keyMap
		}

		// Make an insert into each index
		primaryValue := inserts[primary]["0"]
		for fieldName, values := range inserts {
			for key, value := range values {
				_, _, err = tx.Set(idxKey(idx, fieldName, primaryValue, key), value, nil)
				if err != nil {
					return ErrInternalDBError
				}
			}
		}

		// Index the whole object for returning the whole segment
		_, _, err = tx.Set(idxKey(segmentByPrimaryKey, idx, primaryValue), proto.MarshalTextString(s.segment), nil)
		if err != nil {
			return ErrInternalDBError
		}

		return nil
	})
}

func stringsFromSegmentField(field *api.SegmentField, iter func(key, value string) bool) error {
	switch field.Value.(type) {
	case *api.SegmentField_StringValue:
		_ = iter("0", field.GetStringValue().Value)

	case *api.SegmentField_RepeatedStringValue:
		for key, value := range field.GetRepeatedStringValue().Value {
			if ok := iter(strconv.Itoa(key), value); !ok {
				break
			}
		}

	case *api.SegmentField_IntValue:
		_ = iter("0", strconv.FormatInt(field.GetIntValue().Value, 10))

	case *api.SegmentField_RepeatedIntValue:
		for key, value := range field.GetRepeatedIntValue().Value {
			if ok := iter(strconv.Itoa(key), strconv.FormatInt(value, 10)); !ok {
				break
			}
		}

	case *api.SegmentField_UintValue:
		_ = iter("0", strconv.FormatUint(field.GetUintValue().Value, 10))

	case *api.SegmentField_RepeatedUintValue:
		for key, value := range field.GetRepeatedUintValue().Value {
			if ok := iter(strconv.Itoa(key), strconv.FormatUint(value, 10)); !ok {
				break
			}
		}

	case *api.SegmentField_FloatValue:
		_ = iter("0", strconv.FormatFloat(field.GetFloatValue().Value, 'E', -1, 64))

	case *api.SegmentField_RepeatedFloatValue:
		for key, value := range field.GetRepeatedFloatValue().Value {
			if ok := iter(strconv.Itoa(key), strconv.FormatFloat(value, 'E', -1, 64)); !ok {
				break
			}
		}

	case *api.SegmentField_BoolValue:
		_ = iter("0", strconv.FormatBool(field.GetBoolValue().Value))

	case *api.SegmentField_RepeatedBoolValue:
		for key, value := range field.GetRepeatedBoolValue().Value {
			if ok := iter(strconv.Itoa(key), strconv.FormatBool(value)); !ok {
				break
			}
		}

	case *api.SegmentField_BlobValue:
		_ = iter("0", field.GetStringValue().Value)

	case *api.SegmentField_RepeatedBlobValue:
		for key, value := range field.GetRepeatedStringValue().Value {
			if ok := iter(strconv.Itoa(key), value); !ok {
				break
			}
		}

	case *api.SegmentField_RangeIntValue:
		// We must use infinity to disable one dimension
		_ = iter("0", "[-inf "+strconv.FormatInt(field.GetRangeIntValue().Min, 10)+"], "+
			"[+inf "+strconv.FormatInt(field.GetRangeIntValue().Max, 10)+"]")

	case *api.SegmentField_RepeatedRangeIntValue:
		for key, value := range field.GetRepeatedRangeIntValue().Value {
			if ok := iter(strconv.Itoa(key), "["+strconv.FormatInt(value.Min, 10)+" "+
				strconv.FormatInt(value.Max, 10)+"]"); !ok {
				break
			}
		}

	case *api.SegmentField_RangeFloatValue:
		_ = iter("0", "["+strconv.FormatFloat(field.GetRangeFloatValue().Min, 'E', -1, 64)+" "+
			strconv.FormatFloat(field.GetRangeFloatValue().Max, 'E', -1, 64)+"]")

	case *api.SegmentField_RepeatedRangeFloatValue:
		for key, value := range field.GetRepeatedRangeFloatValue().Value {
			if ok := iter(strconv.Itoa(key), "["+strconv.FormatFloat(value.Min, 'E', -1, 64)+" "+
				strconv.FormatFloat(value.Max, 'E', -1, 64)+"]"); !ok {
				break
			}
		}

	case *api.SegmentField_GeoPointValue:
		_ = iter("0", "["+strconv.FormatFloat(field.GetGeoPointValue().X, 'E', -1, 64)+" "+
			strconv.FormatFloat(field.GetGeoPointValue().Y, 'E', -1, 64)+"]")

	case *api.SegmentField_RepeatedGeoPointValue:
		for key, value := range field.GetRepeatedGeoPointValue().Value {
			if ok := iter(strconv.Itoa(key), "["+strconv.FormatFloat(value.X, 'E', -1, 64)+" "+
				strconv.FormatFloat(value.Y, 'E', -1, 64)+"]"); !ok {
				break
			}
		}

	case *api.SegmentField_GeoRectValue:
		tl := field.GetGeoRectValue().GetTopLeft()
		br := field.GetGeoRectValue().GetBottomRight()

		_ = iter("0", "["+strconv.FormatFloat(tl.X, 'E', -1, 64)+" "+
			strconv.FormatFloat(tl.Y, 'E', -1, 64)+"],["+
			strconv.FormatFloat(br.X, 'E', -1, 64)+" "+
			strconv.FormatFloat(br.Y, 'E', -1, 64)+"]")

	case *api.SegmentField_RepeatedGeoRectValue:
		for key, value := range field.GetRepeatedGeoRectValue().Value {
			tl := value.GetTopLeft()
			br := value.GetBottomRight()

			if ok := iter(strconv.Itoa(key), "["+strconv.FormatFloat(tl.X, 'E', -1, 64)+" "+
				strconv.FormatFloat(tl.Y, 'E', -1, 64)+"],["+
				strconv.FormatFloat(br.X, 'E', -1, 64)+" "+
				strconv.FormatFloat(br.Y, 'E', -1, 64)+"]"); !ok {
				break
			}
		}

	default:
		return ErrFieldUnknown
	}

	return nil
}
