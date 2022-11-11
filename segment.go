package db

import (
	"errors"
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

		// Gather the values by segmentField name and key in a 0 based map
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
			stringer := NewSegmentStringer(field, func(key string, value string) bool {
				keyMap[key] = value
				return true
			})

			if err = stringer.Marshall(); err != nil {
				return ErrInternalDBError
			}

			inserts[field.Name] = keyMap
		}

		// Make an insert into each index
		// TODO do we allow repeated primary? Probably not
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

type Stringer struct {
	segmentField *api.SegmentField
	lookupField  *api.LookupField
	iter         func(key, value string) bool
}

func NewSegmentStringer(field *api.SegmentField, iter func(key, value string) bool) *Stringer {
	return &Stringer{
		segmentField: field,
		iter:         iter,
	}
}

func NewLookupStringer(field *api.LookupField, iter func(key, value string) bool) *Stringer {
	return &Stringer{
		lookupField: field,
		iter:        iter,
	}
}

func (s *Stringer) Marshall() error {
	if s.segmentField != nil {
		return s.marshallSegment()
	}

	if s.lookupField != nil {
		return s.marshallLookup()
	}

	return errors.New("no field set")
}

func (s *Stringer) marshallSegment() error {
	switch s.segmentField.Value.(type) {
	case *api.SegmentField_StringValue:
		s.fromStringValue(0, s.segmentField.GetStringValue().Value)

	case *api.SegmentField_RepeatedStringValue:
		s.fromRepeatedStringValue(s.segmentField.GetRepeatedStringValue().Value)

	case *api.SegmentField_IntValue:
		s.fromIntValue(0, s.segmentField.GetIntValue().Value)

	case *api.SegmentField_RepeatedIntValue:
		s.fromRepeatedIntValue(s.segmentField.GetRepeatedIntValue().Value)

	case *api.SegmentField_UintValue:
		s.fromUintValue(0, s.segmentField.GetUintValue().Value)

	case *api.SegmentField_RepeatedUintValue:
		s.fromRepeatedUintValue(s.segmentField.GetRepeatedUintValue().Value)

	case *api.SegmentField_FloatValue:
		s.fromFloatValue(0, s.segmentField.GetFloatValue().Value)

	case *api.SegmentField_RepeatedFloatValue:
		s.fromRepeatedFloatValue(s.segmentField.GetRepeatedFloatValue().Value)

	case *api.SegmentField_BoolValue:
		s.fromBoolValue(0, s.segmentField.GetBoolValue().Value)

	case *api.SegmentField_RepeatedBoolValue:
		s.fromRepeatedBoolValue(s.segmentField.GetRepeatedBoolValue().Value)

	case *api.SegmentField_BlobValue:
		s.fromBlobValue(0, s.segmentField.GetBlobValue().Value)

	case *api.SegmentField_RepeatedBlobValue:
		s.fromRepeatedBlobValue(s.segmentField.GetRepeatedBlobValue().Value)

	case *api.SegmentField_RangeIntValue:
		s.fromRangeIntValue(0, s.segmentField.GetRangeIntValue())

	case *api.SegmentField_RepeatedRangeIntValue:
		s.fromRepeatedRangeIntValue(s.segmentField.GetRepeatedRangeIntValue().Value)

	case *api.SegmentField_RangeFloatValue:
		s.fromRangeFloatValue(0, s.segmentField.GetRangeFloatValue())

	case *api.SegmentField_RepeatedRangeFloatValue:
		s.fromRepeatedRangeFloatValue(s.segmentField.GetRepeatedRangeFloatValue().Value)

	case *api.SegmentField_GeoPointValue:
		s.fromGeoPointValue(0, s.segmentField.GetGeoPointValue())

	case *api.SegmentField_RepeatedGeoPointValue:
		s.fromRepeatedGeoPointValue(s.segmentField.GetRepeatedGeoPointValue().Value)

	case *api.SegmentField_GeoRectValue:
		s.fromGeoRectValue(0, s.segmentField.GetGeoRectValue())

	case *api.SegmentField_RepeatedGeoRectValue:
		s.fromRepeatedGeoRectValue(s.segmentField.GetRepeatedGeoRectValue().Value)

	default:
		return ErrFieldUnknown
	}
	return nil
}

func (s *Stringer) marshallLookup() error {
	switch s.lookupField.Value.(type) {
	case *api.LookupField_StringValue:
		s.fromStringValue(0, s.lookupField.GetStringValue().Value)

	case *api.LookupField_RepeatedStringValue:
		s.fromRepeatedStringValue(s.lookupField.GetRepeatedStringValue().Value)

	case *api.LookupField_IntValue:
		s.fromIntValue(0, s.lookupField.GetIntValue().Value)

	case *api.LookupField_RepeatedIntValue:
		s.fromRepeatedIntValue(s.lookupField.GetRepeatedIntValue().Value)

	case *api.LookupField_UintValue:
		s.fromUintValue(0, s.lookupField.GetUintValue().Value)

	case *api.LookupField_RepeatedUintValue:
		s.fromRepeatedUintValue(s.lookupField.GetRepeatedUintValue().Value)

	case *api.LookupField_FloatValue:
		s.fromFloatValue(0, s.lookupField.GetFloatValue().Value)

	case *api.LookupField_RepeatedFloatValue:
		s.fromRepeatedFloatValue(s.lookupField.GetRepeatedFloatValue().Value)

	case *api.LookupField_BoolValue:
		s.fromBoolValue(0, s.lookupField.GetBoolValue().Value)

	case *api.LookupField_RepeatedBoolValue:
		s.fromRepeatedBoolValue(s.lookupField.GetRepeatedBoolValue().Value)

	case *api.LookupField_RangeIntValue:
		s.fromRangeIntValue(0, s.lookupField.GetRangeIntValue())

	case *api.LookupField_RepeatedRangeIntValue:
		s.fromRepeatedRangeIntValue(s.lookupField.GetRepeatedRangeIntValue().Value)

	case *api.LookupField_RangeFloatValue:
		s.fromRangeFloatValue(0, s.lookupField.GetRangeFloatValue())

	case *api.LookupField_RepeatedRangeFloatValue:
		s.fromRepeatedRangeFloatValue(s.lookupField.GetRepeatedRangeFloatValue().Value)

	case *api.LookupField_GeoPointValue:
		s.fromGeoPointValue(0, s.lookupField.GetGeoPointValue())

	case *api.LookupField_RepeatedGeoPointValue:
		s.fromRepeatedGeoPointValue(s.lookupField.GetRepeatedGeoPointValue().Value)

	case *api.LookupField_GeoRectValue:
		s.fromGeoRectValue(0, s.lookupField.GetGeoRectValue())

	case *api.LookupField_RepeatedGeoRectValue:
		s.fromRepeatedGeoRectValue(s.lookupField.GetRepeatedGeoRectValue().Value)

	default:
		return ErrFieldUnknown
	}
	return nil
}

func (s *Stringer) fromStringValue(key int, value string) bool {
	return s.iter(strconv.Itoa(key), value)
}

func (s *Stringer) fromRepeatedStringValue(values []string) {
	for key, value := range values {
		if ok := s.fromStringValue(key, value); !ok {
			break
		}
	}
}

func (s *Stringer) fromIntValue(key int, value int64) bool {
	return s.iter(strconv.Itoa(key), strconv.FormatInt(value, 10))
}

func (s *Stringer) fromRepeatedIntValue(values []int64) {
	for key, value := range values {
		if ok := s.fromIntValue(key, value); !ok {
			break
		}
	}
}

func (s *Stringer) fromUintValue(key int, value uint64) bool {
	return s.iter(strconv.Itoa(key), strconv.FormatUint(value, 10))
}

func (s *Stringer) fromRepeatedUintValue(values []uint64) {
	for key, value := range values {
		if ok := s.fromUintValue(key, value); !ok {
			break
		}
	}
}

func (s *Stringer) fromFloatValue(key int, value float64) bool {
	return s.iter(strconv.Itoa(key), strconv.FormatFloat(value, 'E', -1, 64))
}

func (s *Stringer) fromRepeatedFloatValue(values []float64) {
	for key, value := range values {
		if ok := s.fromFloatValue(key, value); !ok {
			break
		}
	}
}

func (s *Stringer) fromBoolValue(key int, value bool) bool {
	return s.iter(strconv.Itoa(key), strconv.FormatBool(value))
}

func (s *Stringer) fromRepeatedBoolValue(values []bool) {
	for key, value := range values {
		if ok := s.fromBoolValue(key, value); !ok {
			break
		}
	}
}

func (s *Stringer) fromBlobValue(key int, value string) bool {
	return s.fromStringValue(key, value)
}

func (s *Stringer) fromRepeatedBlobValue(values []string) {
	s.fromRepeatedStringValue(values)
}

func (s *Stringer) fromRangeIntValue(key int, value *api.SegmentFieldRangeInt) bool {
	// We must use infinity to disable one dimension
	return s.iter(strconv.Itoa(key), "[-inf "+strconv.FormatInt(value.Min, 10)+"], "+
		"[+inf "+strconv.FormatInt(value.Max, 10)+"]")
}

func (s *Stringer) fromRepeatedRangeIntValue(values []*api.SegmentFieldRangeInt) {
	for key, value := range values {
		if ok := s.fromRangeIntValue(key, value); !ok {
			break
		}
	}
}

func (s *Stringer) fromRangeFloatValue(key int, value *api.SegmentFieldRangeFloat) bool {
	// We must use infinity to disable one dimension
	return s.iter(strconv.Itoa(key), "[-inf "+strconv.FormatFloat(value.Min, 'E', -1, 64)+"], "+
		"[+inf "+strconv.FormatFloat(value.Max, 'E', -1, 64)+"]")
}

func (s *Stringer) fromRepeatedRangeFloatValue(values []*api.SegmentFieldRangeFloat) {
	for key, value := range values {
		if ok := s.fromRangeFloatValue(key, value); !ok {
			break
		}
	}
}

func (s *Stringer) fromGeoPointValue(key int, value *api.SegmentFieldGeoPoint) bool {
	return s.iter(strconv.Itoa(key), "["+strconv.FormatFloat(value.X, 'E', -1, 64)+" "+
		strconv.FormatFloat(value.Y, 'E', -1, 64)+"]")
}

func (s *Stringer) fromRepeatedGeoPointValue(values []*api.SegmentFieldGeoPoint) {
	for key, value := range values {
		if ok := s.fromGeoPointValue(key, value); !ok {
			break
		}
	}
}

func (s *Stringer) fromGeoRectValue(key int, value *api.SegmentFieldGeoRect) bool {
	tl := value.GetTopLeft()
	br := value.GetBottomRight()

	return s.iter(strconv.Itoa(key), "["+strconv.FormatFloat(tl.X, 'E', -1, 64)+" "+
		strconv.FormatFloat(tl.Y, 'E', -1, 64)+"],["+
		strconv.FormatFloat(br.X, 'E', -1, 64)+" "+
		strconv.FormatFloat(br.Y, 'E', -1, 64)+"]")
}

func (s *Stringer) fromRepeatedGeoRectValue(values []*api.SegmentFieldGeoRect) {
	for key, value := range values {
		if ok := s.fromGeoRectValue(key, value); !ok {
			break
		}
	}
}
