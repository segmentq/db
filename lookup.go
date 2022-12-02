package db

import (
	"github.com/golang/protobuf/proto"
	api "github.com/segmentq/protos-api-go"
	"github.com/tidwall/buntdb"
	"google.golang.org/api/iterator"
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
		indexId, err := tx.Get(idxKey(idxById, t.idx), true)
		if err != nil {
			return ErrInternalDBError
		}

		return t.scanAllFields(indexId, m, tx)
	})

	if err != nil {
		return err
	}

	// Work out our matches across all keys
	t.keys = m.getMatches()
	return nil
}

// scanAllFields iterates through each lookup field to hydrate the matcher
func (t *Iterator) scanAllFields(indexId string, m *matcher, tx *buntdb.Tx) error {
	// For each of the lookup fields, scan the indexes
	for _, field := range t.l.lookup.Fields {
		m.setField(field)

		s := NewLookupStringer(field, func(_, value string) bool {
			if isGeoLookupField(field) {
				return tx.Intersects(idxKey(indexId, field.Name), value, m.match) == nil
			}
			return tx.AscendEqual(idxKey(indexId, field.Name), value, m.match) == nil
		})

		if err := s.MarshallText(); err != nil {
			return ErrLookupFailure
		}

		if m.getMatchCount() == 0 {
			return ErrLookupEmpty
		}
	}

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
