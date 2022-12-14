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
	index, err := db.GetIndexByName(indexName)
	if err != nil {
		return nil, err
	}
	s := newSegment(db, index, segment)
	return s, s.insertToIndexName(indexName)
}

func (db *DB) NewSegment(indexName string, segment *api.Segment) (*Segment, error) {
	index, err := db.GetIndexByName(indexName)
	if err != nil {
		return nil, err
	}
	return newSegment(db, index, segment), nil
}

func (i *Index) NewSegment(segment *api.Segment) (*Segment, error) {
	return newSegment(i.db, i, segment), nil
}

func (i *Index) InsertSegment(segment *api.Segment) (*Segment, error) {
	s := newSegment(i.db, i, segment)
	return s, s.Insert()
}

func (db *DB) GetSegmentByKey(indexName string, segmentKey string) (*Segment, error) {
	index, err := db.GetIndexByName(indexName)
	if err != nil {
		return nil, err
	}

	return index.GetSegmentByKey(segmentKey)
}

func (i *Index) GetSegmentByKey(key string) (*Segment, error) {
	var s string
	err := i.db.engine.View(func(tx *buntdb.Tx) error {
		idx, err := tx.Get(idxKey(idxById, i.definition.Name), true)
		if err != nil {
			return ErrInternalDBError
		}

		s, err = tx.Get(idxKey(segmentByPrimaryKey, idx, key))
		if err != nil {
			return ErrSegmentNotFound
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	var segment api.Segment
	err = proto.UnmarshalText(s, &segment)

	if err != nil {
		return nil, ErrMarshallingFailed
	}

	return &Segment{
		db:      i.db,
		index:   i,
		segment: &segment,
	}, nil
}

func (db *DB) GetAllSegments(indexName string, iter func(segment *api.Segment) bool) error {
	i, err := db.GetIndexByName(indexName)
	if err != nil {
		return err
	}

	return i.GetAllSegments(iter)
}

func (i *Index) GetAllSegments(iter func(segment *api.Segment) bool) error {
	return i.db.engine.View(func(tx *buntdb.Tx) error {
		idx, err := tx.Get(idxKey(idxById, i.definition.Name), true)
		if err != nil {
			return ErrInternalDBError
		}

		if err = tx.Ascend(idxKey(segmentByPrimaryKey, idx), func(key, value string) bool {
			var s api.Segment
			if err2 := proto.UnmarshalText(value, &s); err2 != nil {
				return false
			}

			return iter(&s)
		}); err != nil {
			return ErrInternalDBError
		}

		return nil
	})
}

func (db *DB) DeleteSegment(indexName string, segmentKey string) (*Segment, error) {
	index, err := db.GetIndexByName(indexName)
	if err != nil {
		return nil, err
	}

	return index.DeleteSegment(segmentKey)
}

func (i *Index) DeleteSegment(key string) (*Segment, error) {
	segment, err := i.GetSegmentByKey(key)
	if err != nil {
		return nil, err
	}

	err = segment.deleteFromIndexName(i.definition.Name)
	if err != nil {
		return nil, err
	}

	return segment, nil
}

func (s *Segment) Delete() error {
	return s.deleteFromIndexName(s.index.definition.Name)
}

type deleteSegmentTxn struct {
	indexName string
	key       string
	valueMap  map[string]map[string]string
	segment   *api.Segment
}

func newDeleteSegmentTxn(indexName string, key string, valueMap map[string]map[string]string, segment *api.Segment) *deleteSegmentTxn {
	return &deleteSegmentTxn{
		indexName: indexName,
		key:       key,
		valueMap:  valueMap,
		segment:   segment,
	}
}

func (t *deleteSegmentTxn) call(tx *buntdb.Tx) error {
	// Find the integer index of the index
	idx, err := tx.Get(idxKey(idxById, t.indexName), true)
	if err != nil {
		return ErrInternalDBError
	}

	// Delete from each index
	for fieldName, values := range t.valueMap {
		for key := range values {
			_, err = tx.Delete(idxKey(idx, fieldName, t.key, key))
			if err != nil {
				return ErrInternalDBError
			}
		}
	}

	// Delete the whole object
	_, err = tx.Delete(idxKey(segmentByPrimaryKey, idx, t.key))
	if err != nil {
		return ErrInternalDBError
	}

	return nil
}

func (s *Segment) deleteFromIndexName(indexName string) error {
	primary, deletes, err := s.generateIndexMap(indexName)
	if err != nil {
		return err
	}

	key := deletes[primary]["0"]

	txn := NewTxn(s.db, true)
	txn.AddAction(newDeleteSegmentTxn(indexName, key, deletes, s.segment))

	return txn.Settle()
}

// ReplaceSegment TODO change this to take the Segment as an arg instead of proto
func (db *DB) ReplaceSegment(indexName string, segmentKey string, newSegment *api.Segment) (*Segment, error) {
	index, err := db.GetIndexByName(indexName)
	if err != nil {
		return nil, err
	}

	return index.ReplaceSegment(segmentKey, newSegment)
}

func (i *Index) ReplaceSegment(key string, newSegment *api.Segment) (*Segment, error) {
	segment, err := i.GetSegmentByKey(key)
	if err != nil {
		return nil, err
	}

	return segment.replaceInIndexName(i.definition.Name, newSegment)
}

func (s *Segment) Replace(new *api.Segment) (*Segment, error) {
	return s.replaceInIndexName(s.index.definition.Name, new)
}

func (s *Segment) replaceInIndexName(indexName string, new *api.Segment) (*Segment, error) {
	r := &Segment{
		db:      s.db,
		index:   s.index,
		segment: new,
	}

	primary, deletes, err := s.generateIndexMap(indexName)
	if err != nil {
		return nil, err
	}

	_, inserts, err := r.generateIndexMap(indexName)
	if err != nil {
		return nil, err
	}

	deleteKey := deletes[primary]["0"]
	insertKey := inserts[primary]["0"]

	txn := NewTxn(s.db, true)
	txn.AddAction(newDeleteSegmentTxn(indexName, deleteKey, deletes, s.segment))
	txn.AddAction(newInsertSegmentTxn(indexName, insertKey, inserts, r.segment))

	if err = txn.Settle(); err != nil {
		return nil, err
	}

	return r, nil
}

func newSegment(db *DB, index *Index, segment *api.Segment) *Segment {
	return &Segment{
		db:      db,
		index:   index,
		segment: segment,
	}
}

func (s *Segment) Insert() error {
	return s.insertToIndexName(s.index.definition.Name)
}

func (s *Segment) Proto() *api.Segment {
	return s.segment
}

type insertSegmentTxn struct {
	indexName string
	key       string
	valueMap  map[string]map[string]string
	segment   *api.Segment
}

func newInsertSegmentTxn(indexName string, key string, valueMap map[string]map[string]string, segment *api.Segment) *insertSegmentTxn {
	return &insertSegmentTxn{
		indexName: indexName,
		key:       key,
		valueMap:  valueMap,
		segment:   segment,
	}
}

func (t *insertSegmentTxn) call(tx *buntdb.Tx) error {
	// Find the integer index of the index
	idx, err := tx.Get(idxKey(idxById, t.indexName), true)
	if err != nil {
		return ErrInternalDBError
	}

	// Make an insert into each index
	// TODO do we allow repeated primary? Probably not
	for fieldName, values := range t.valueMap {
		for key, value := range values {
			_, _, err = tx.Set(idxKey(idx, fieldName, t.key, key), value, nil)
			if err != nil {
				return ErrInternalDBError
			}
		}
	}

	// Index the whole object for returning the whole segment
	_, _, err = tx.Set(idxKey(segmentByPrimaryKey, idx, t.key), proto.MarshalTextString(t.segment), nil)
	if err != nil {
		return ErrInternalDBError
	}

	return nil
}

func (s *Segment) insertToIndexName(indexName string) error {
	primary, inserts, err := s.generateIndexMap(indexName)
	if err != nil {
		return err
	}

	key := inserts[primary]["0"]

	txn := NewTxn(s.db, true)
	txn.AddAction(newInsertSegmentTxn(indexName, key, inserts, s.segment))

	return txn.Settle()
}

func (s *Segment) generateIndexMap(indexName string) (primary string, inserts map[string]map[string]string, err error) {
	// Gather the values by field name and key in a 0 based map
	inserts = make(map[string]map[string]string, 0)

	for _, field := range s.segment.Fields {
		definition, exists := s.db.fields[indexName][field.Name]
		if !exists {
			if _, ok := s.db.fields[indexName]; !ok {
				return "", nil, ErrIndexUnknown
			}
			return "", nil, ErrFieldUnknown
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

		if err = stringer.MarshallText(); err != nil {
			return "", nil, ErrInternalDBError
		}

		inserts[field.Name] = keyMap
	}

	return primary, inserts, nil
}

type Stringer struct {
	segmentField    *api.SegmentField
	lookupField     *api.LookupField
	fieldDefinition *api.FieldDefinition
	iter            func(key, value string) bool
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

func NewFieldDefinitionStringer(fieldDefinition *api.FieldDefinition) *Stringer {
	return &Stringer{
		fieldDefinition: fieldDefinition,
	}
}

func (s *Stringer) MarshallText() error {
	if s.segmentField != nil {
		return s.marshallSegment()
	}

	if s.lookupField != nil {
		return s.marshallLookup()
	}

	return errors.New("no field set")
}

func (s *Stringer) UnmarshallText(value string) (*api.SegmentField, error) {
	if s.fieldDefinition != nil {
		return s.unmarshallSegmentFieldText(value)
	}

	return nil, errors.New("no field definition was provided")
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

func (s *Stringer) unmarshallSegmentFieldText(value string) (segmentField *api.SegmentField, err error) {
	switch s.fieldDefinition.DataType.(type) {
	case *api.FieldDefinition_Scalar:
		segmentField, err = s.unmarshallScalarFieldText(value)
	case *api.FieldDefinition_Geo:
		segmentField, err = s.unmarshallGeoFieldText(value)
	default:
		return nil, ErrFieldUnknown
	}

	if err != nil {
		return nil, err
	}

	return segmentField, nil
}

func (s *Stringer) unmarshallScalarFieldText(value string) (segmentField *api.SegmentField, err error) {
	switch s.fieldDefinition.GetScalar() {
	// STRINGs + BLOBs
	case api.ScalarType_DATA_TYPE_UNDEFINED, api.ScalarType_DATA_TYPE_STRING, api.ScalarType_DATA_TYPE_BLOB:
		return s.toStringField(value)
	// INTs
	case api.ScalarType_DATA_TYPE_INT, api.ScalarType_DATA_TYPE_INT64, api.ScalarType_DATA_TYPE_INT8,
		api.ScalarType_DATA_TYPE_INT16, api.ScalarType_DATA_TYPE_INT32:
		return s.toIntField(value)
	// UINTs
	case api.ScalarType_DATA_TYPE_UINT, api.ScalarType_DATA_TYPE_UINT64, api.ScalarType_DATA_TYPE_UINT8,
		api.ScalarType_DATA_TYPE_UINT16, api.ScalarType_DATA_TYPE_UINT32:
		return s.toUintField(value)
	// FLOATs
	case api.ScalarType_DATA_TYPE_FLOAT, api.ScalarType_DATA_TYPE_FLOAT64, api.ScalarType_DATA_TYPE_FLOAT32:
		return s.toFloatField(value)
	// BOOL
	case api.ScalarType_DATA_TYPE_BOOL:
		return s.toBoolField(value)
	}

	return nil, ErrFieldUnknown
}

func (s *Stringer) unmarshallGeoFieldText(value string) (segmentField *api.SegmentField, err error) {
	return nil, errors.New("not implemented")

	//switch s.fieldDefinition.GetGeo() {
	//// RANGEs
	//case api.GeoType_DATA_TYPE_RANGE, api.GeoType_DATA_TYPE_RANGE_FLOAT:
	//	return s.toRangeFloatField(value)
	//case api.GeoType_DATA_TYPE_RANGE_INT:
	//	return s.toRangeIntField(value)
	//// GEOs
	//case api.GeoType_DATA_TYPE_GEO, api.GeoType_DATA_TYPE_GEO_POINT:
	//	return s.toGeoPointField(value)
	//case api.GeoType_DATA_TYPE_GEO_RECT:
	//	return s.toGeoRectField(value)
	//}
	//
	//return nil, ErrFieldUnknown
}

func (s *Stringer) fromStringValue(key int, value string) bool {
	return s.iter(strconv.Itoa(key), value)
}

func (s *Stringer) toStringField(value string) (*api.SegmentField, error) {
	return &api.SegmentField{
		Name: s.fieldDefinition.Name,
		Value: &api.SegmentField_StringValue{
			StringValue: &api.SegmentFieldString{
				Value: value,
			},
		},
	}, nil
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

func (s *Stringer) toIntField(value string) (*api.SegmentField, error) {
	converted, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, err
	}

	return &api.SegmentField{
		Name: s.fieldDefinition.Name,
		Value: &api.SegmentField_IntValue{
			IntValue: &api.SegmentFieldInt{
				Value: converted,
			},
		},
	}, nil
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

func (s *Stringer) toUintField(value string) (*api.SegmentField, error) {
	converted, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, err
	}

	return &api.SegmentField{
		Name: s.fieldDefinition.Name,
		Value: &api.SegmentField_UintValue{
			UintValue: &api.SegmentFieldUInt{
				Value: converted,
			},
		},
	}, nil
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

func (s *Stringer) toFloatField(value string) (*api.SegmentField, error) {
	converted, err := strconv.ParseFloat(value, 10)
	if err != nil {
		return nil, err
	}

	return &api.SegmentField{
		Name: s.fieldDefinition.Name,
		Value: &api.SegmentField_FloatValue{
			FloatValue: &api.SegmentFieldFloat{
				Value: converted,
			},
		},
	}, nil
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

func (s *Stringer) toBoolField(value string) (*api.SegmentField, error) {
	converted, err := strconv.ParseBool(value)
	if err != nil {
		return nil, err
	}

	return &api.SegmentField{
		Name: s.fieldDefinition.Name,
		Value: &api.SegmentField_BoolValue{
			BoolValue: &api.SegmentFieldBool{
				Value: converted,
			},
		},
	}, nil
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
