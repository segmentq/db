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

		if err = stringer.Marshall(); err != nil {
			return "", nil, ErrInternalDBError
		}

		inserts[field.Name] = keyMap
	}

	return primary, inserts, nil
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
