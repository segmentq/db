package db

import (
	"github.com/golang/protobuf/proto"
	api "github.com/segmentq/protos-api-go"
	"github.com/tidwall/buntdb"
	"google.golang.org/api/iterator"
	"strconv"
)

type Lookup struct {
	db       *DB
	index    *Index
	lookup   *api.Lookup
	keysOnly bool
}

func newLookup(db *DB, index *Index, lookup *api.Lookup, keysOnly bool) *Lookup {
	return &Lookup{
		db:       db,
		index:    index,
		lookup:   lookup,
		keysOnly: keysOnly,
	}
}

// Lookup is a convenience method on the db object which only returns segment keys
func (db *DB) Lookup(indexName string, lookup *api.Lookup) (*Iterator, error) {
	return db.lookup(indexName, lookup, true)
}

// LookupSegments returns full segment objects and is slower than Lookup
func (db *DB) LookupSegments(indexName string, lookup *api.Lookup) (*Iterator, error) {
	return db.lookup(indexName, lookup, false)
}

// Lookup is a convenience method on the index object which only returns segment keys
func (i *Index) Lookup(lookup *api.Lookup) (*Iterator, error) {
	return i.db.lookup(i.definition.Name, lookup, true)
}

// LookupSegments returns full segment objects and is slower than Lookup
func (i *Index) LookupSegments(lookup *api.Lookup) (*Iterator, error) {
	return i.db.lookup(i.definition.Name, lookup, false)
}

func (db *DB) lookup(indexName string, lookup *api.Lookup, keysOnly bool) (*Iterator, error) {
	l := newLookup(db, nil, lookup, keysOnly)
	it := l.RunOnIndex(indexName)

	if it.err != nil {
		return nil, it.err
	}

	return it, nil
}

func (l *Lookup) Run() *Iterator {
	if l.index == nil {
		return &Iterator{err: ErrIndexNotSet}
	}

	return l.RunOnIndex(l.index.definition.Name)
}

func (l *Lookup) RunOnIndex(indexName string) *Iterator {
	// TODO limit?
	return &Iterator{idx: indexName, l: l}
}

type Iterator struct {
	idx  string
	l    *Lookup
	err  error
	keys []string
}

func (t *Iterator) Next(dst *api.Segment) (key string, err error) {
	key, src, err := t.next()
	if err != nil {
		return "", err
	}
	if dst != nil && !t.l.keysOnly {
		proto.Merge(dst, src)
	}
	return key, err
}

func (t *Iterator) next() (key string, segment *api.Segment, err error) {
	// Lookup keys while there are no results
	for t.err == nil && len(t.keys) == 0 {
		t.err = t.lookup()
	}

	if t.err != nil {
		return "", nil, t.err
	}

	key = t.keys[0]
	t.keys = t.keys[1:]

	if len(t.keys) == 0 {
		t.err = iterator.Done // At the end of the batch.
	}

	if t.l.keysOnly {
		return key, nil, nil
	}

	err = t.l.db.engine.View(func(tx *buntdb.Tx) error {
		segmentText, err := tx.Get(idxKey(segmentByPrimaryKey, t.idx, key))
		if err != nil {
			return ErrSegmentMissing
		}
		err = proto.UnmarshalText(segmentText, segment)
		if err != nil {
			return ErrSegmentMissing
		}
		return nil
	})

	if err != nil {
		return "", nil, err
	}

	return key, segment, nil
}

type matcher struct {
	field     *api.LookupField
	refKeyMap map[string]string // The reference key map
	refKeySet []string          // The reference key set
	keyMap    map[string]string
	keySet    []string
}

func newMatcher() *matcher {
	return &matcher{}
}

func (m *matcher) setField(field *api.LookupField) {
	// Any existing key maps become reference
	m.refKeyMap = m.keyMap
	m.refKeySet = m.keySet
	m.field = field
	m.keyMap = make(map[string]string)
	m.keySet = make([]string, 0)
}

func (m *matcher) match(key, _ string) bool {
	keyObject := keyFromString(key)
	k, exists := keyObject.SegmentKey()
	if !exists {
		return false
	}

	// Check if the key was matched on the previous field, if so add to the new map
	// OR refKeySet length is 0 which means we are on the first field
	if _, ok := m.refKeyMap[k]; ok || len(m.refKeySet) == 0 {
		m.keyMap[k] = m.field.Name
		m.keySet = append(m.keySet, k)
	}

	return true
}

func (m *matcher) getMatchCount() int {
	return len(m.keySet)
}

func (m *matcher) getMatches() []string {
	return m.keySet
}

func (t *Iterator) lookup() error {
	if t.err != nil {
		return t.err
	}

	// Start a matcher to hold the results of the index matches
	m := newMatcher()

	err := t.l.db.engine.View(func(tx *buntdb.Tx) error {
		// Find the integer index of the index
		// TODO can we store this in DB struct?
		idx, err := tx.Get(idxKey(idxById, t.idx), true)
		if err != nil {
			return ErrInternalDBError
		}

		// For each of the lookup fields, scan the indexes
		for _, field := range t.l.lookup.Fields {
			m.setField(field)

			err = stringsFromLookupField(field, func(_, value string) bool {
				if isGeoLookupField(field) {
					return tx.Intersects(idxKey(idx, field.Name), value, m.match) == nil
				}
				return tx.AscendEqual(idxKey(idx, field.Name), value, m.match) == nil
			})

			if err != nil {
				return ErrLookupFailure
			}

			if m.getMatchCount() == 0 {
				return ErrLookupEmpty
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Work out our matches across all keys
	t.keys = m.getMatches()
	return nil
}

func isGeoLookupField(field *api.LookupField) bool {
	switch field.Value.(type) {
	case *api.LookupField_RangeIntValue,
		*api.LookupField_RangeFloatValue,
		*api.LookupField_GeoPointValue,
		*api.LookupField_GeoRectValue,
		*api.LookupField_RepeatedRangeIntValue,
		*api.LookupField_RepeatedRangeFloatValue,
		*api.LookupField_RepeatedGeoPointValue,
		*api.LookupField_RepeatedGeoRectValue:
		return true
	}

	return false
}

func stringsFromLookupField(field *api.LookupField, iter func(key, value string) bool) error {
	switch field.Value.(type) {
	case *api.LookupField_StringValue:
		_ = iter("0", field.GetStringValue().Value)

	case *api.LookupField_RepeatedStringValue:
		for key, value := range field.GetRepeatedStringValue().Value {
			if ok := iter(strconv.Itoa(key), value); !ok {
				break
			}
		}

	case *api.LookupField_IntValue:
		_ = iter("0", strconv.FormatInt(field.GetIntValue().Value, 10))

	case *api.LookupField_RepeatedIntValue:
		for key, value := range field.GetRepeatedIntValue().Value {
			if ok := iter(strconv.Itoa(key), strconv.FormatInt(value, 10)); !ok {
				break
			}
		}

	case *api.LookupField_UintValue:
		_ = iter("0", strconv.FormatUint(field.GetUintValue().Value, 10))

	case *api.LookupField_RepeatedUintValue:
		for key, value := range field.GetRepeatedUintValue().Value {
			if ok := iter(strconv.Itoa(key), strconv.FormatUint(value, 10)); !ok {
				break
			}
		}

	case *api.LookupField_FloatValue:
		_ = iter("0", strconv.FormatFloat(field.GetFloatValue().Value, 'E', -1, 64))

	case *api.LookupField_RepeatedFloatValue:
		for key, value := range field.GetRepeatedFloatValue().Value {
			if ok := iter(strconv.Itoa(key), strconv.FormatFloat(value, 'E', -1, 64)); !ok {
				break
			}
		}

	case *api.LookupField_BoolValue:
		_ = iter("0", strconv.FormatBool(field.GetBoolValue().Value))

	case *api.LookupField_RepeatedBoolValue:
		for key, value := range field.GetRepeatedBoolValue().Value {
			if ok := iter(strconv.Itoa(key), strconv.FormatBool(value)); !ok {
				break
			}
		}

	case *api.LookupField_RangeIntValue:
		// We must use infinity to disable one dimension
		_ = iter("0", "[-inf "+strconv.FormatInt(field.GetRangeIntValue().Min, 10)+"], "+
			"[+inf "+strconv.FormatInt(field.GetRangeIntValue().Max, 10)+"]")

	case *api.LookupField_RepeatedRangeIntValue:
		for key, value := range field.GetRepeatedRangeIntValue().Value {
			if ok := iter(strconv.Itoa(key), "["+strconv.FormatInt(value.Min, 10)+" "+
				strconv.FormatInt(value.Max, 10)+"]"); !ok {
				break
			}
		}

	case *api.LookupField_RangeFloatValue:
		_ = iter("0", "["+strconv.FormatFloat(field.GetRangeFloatValue().Min, 'E', -1, 64)+" "+
			strconv.FormatFloat(field.GetRangeFloatValue().Max, 'E', -1, 64)+"]")

	case *api.LookupField_RepeatedRangeFloatValue:
		for key, value := range field.GetRepeatedRangeFloatValue().Value {
			if ok := iter(strconv.Itoa(key), "["+strconv.FormatFloat(value.Min, 'E', -1, 64)+" "+
				strconv.FormatFloat(value.Max, 'E', -1, 64)+"]"); !ok {
				break
			}
		}

	case *api.LookupField_GeoPointValue:
		_ = iter("0", "["+strconv.FormatFloat(field.GetGeoPointValue().X, 'E', -1, 64)+" "+
			strconv.FormatFloat(field.GetGeoPointValue().Y, 'E', -1, 64)+"]")

	case *api.LookupField_RepeatedGeoPointValue:
		for key, value := range field.GetRepeatedGeoPointValue().Value {
			if ok := iter(strconv.Itoa(key), "["+strconv.FormatFloat(value.X, 'E', -1, 64)+" "+
				strconv.FormatFloat(value.Y, 'E', -1, 64)+"]"); !ok {
				break
			}
		}

	case *api.LookupField_GeoRectValue:
		tl := field.GetGeoRectValue().GetTopLeft()
		br := field.GetGeoRectValue().GetBottomRight()

		_ = iter("0", "["+strconv.FormatFloat(tl.X, 'E', -1, 64)+" "+
			strconv.FormatFloat(tl.Y, 'E', -1, 64)+"],["+
			strconv.FormatFloat(br.X, 'E', -1, 64)+" "+
			strconv.FormatFloat(br.Y, 'E', -1, 64)+"]")

	case *api.LookupField_RepeatedGeoRectValue:
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
