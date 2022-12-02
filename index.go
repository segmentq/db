package db

import (
	"github.com/golang/protobuf/proto"
	api "github.com/segmentq/protos-api-go"
	"github.com/tidwall/buntdb"
	"strconv"
)

var (
	fieldMapScalar = map[api.ScalarType]func(a, b string) bool{
		api.ScalarType_DATA_TYPE_UNDEFINED: buntdb.IndexString,
		// STRINGs
		api.ScalarType_DATA_TYPE_STRING: buntdb.IndexString,
		// INTs
		api.ScalarType_DATA_TYPE_INT:   buntdb.IndexInt,
		api.ScalarType_DATA_TYPE_INT8:  buntdb.IndexInt,
		api.ScalarType_DATA_TYPE_INT16: buntdb.IndexInt,
		api.ScalarType_DATA_TYPE_INT32: buntdb.IndexInt,
		api.ScalarType_DATA_TYPE_INT64: buntdb.IndexInt,
		// UINTs
		api.ScalarType_DATA_TYPE_UINT:   buntdb.IndexUint,
		api.ScalarType_DATA_TYPE_UINT8:  buntdb.IndexUint,
		api.ScalarType_DATA_TYPE_UINT16: buntdb.IndexUint,
		api.ScalarType_DATA_TYPE_UINT32: buntdb.IndexUint,
		api.ScalarType_DATA_TYPE_UINT64: buntdb.IndexUint,
		// FLOATs
		api.ScalarType_DATA_TYPE_FLOAT:   buntdb.IndexFloat,
		api.ScalarType_DATA_TYPE_FLOAT32: buntdb.IndexFloat,
		api.ScalarType_DATA_TYPE_FLOAT64: buntdb.IndexFloat,
		// BOOL
		api.ScalarType_DATA_TYPE_BOOL: buntdb.IndexBinary,
	}
	fieldMapGeo = map[api.GeoType]func(a string) (min, max []float64){
		// RANGEs
		api.GeoType_DATA_TYPE_RANGE:       buntdb.IndexRect,
		api.GeoType_DATA_TYPE_RANGE_INT:   buntdb.IndexRect,
		api.GeoType_DATA_TYPE_RANGE_FLOAT: buntdb.IndexRect,
		// GEOs
		api.GeoType_DATA_TYPE_GEO:       buntdb.IndexRect,
		api.GeoType_DATA_TYPE_GEO_RECT:  buntdb.IndexRect,
		api.GeoType_DATA_TYPE_GEO_POINT: buntdb.IndexRect,
	}
)

type Index struct {
	db         *DB
	definition *api.IndexDefinition
}

// CreateIndex takes an IndexDefinition and returns an Index
func (db *DB) CreateIndex(indexDefinition *api.IndexDefinition) (*Index, error) {
	index := newIndex(db, indexDefinition)
	if err := index.Create(); err != nil {
		return nil, err
	}
	return index, nil
}

// TruncateIndex is a convenience method to call the Truncate method of an Index, which removes all segments
func (db *DB) TruncateIndex(name string) error {
	definition, ok := db.idx[name]
	if !ok {
		return ErrIndexUnknown
	}

	index := &Index{
		db:         db,
		definition: definition,
	}
	return index.Truncate()
}

// DeleteIndex deletes all segments and the index itself
func (db *DB) DeleteIndex(name string) (*Index, error) {
	index, err := db.GetIndexByName(name)
	if err != nil {
		return nil, err
	}

	return index, index.Delete()
}

// GetIndexByName returns the index with the specified name
func (db *DB) GetIndexByName(name string) (*Index, error) {
	definition, ok := db.idx[name]
	if !ok {
		return nil, ErrIndexUnknown
	}

	return &Index{
		db:         db,
		definition: definition,
	}, nil
}

// ListIndexes returns an unbuffered list of all indexes TODO maybe some limit to this?
func (db *DB) ListIndexes() []*api.IndexDefinition {
	list := make([]*api.IndexDefinition, 0, len(db.idx))

	for _, index := range db.idx {
		list = append(list, index)
	}

	return list
}

func newIndex(db *DB, definition *api.IndexDefinition) *Index {
	return &Index{
		db:         db,
		definition: definition,
	}
}

func (i *Index) Exists() (bool, error) {
	if i.definition == nil || i.definition.Name == "" {
		return false, ErrIndexUnknown
	}

	_, exists := i.db.idx[i.definition.Name]
	return exists, nil
}

// Create is used when the Index is instantiated directly
func (i *Index) Create() error {
	exists, err := i.Exists()
	if err != nil {
		return err
	}
	if exists {
		return ErrIndexExists
	}

	var idStr string
	err = i.db.engine.Update(func(tx *buntdb.Tx) error {
		// Determine the last insert id
		id := 0
		dbSize, err2 := tx.Len()
		if err2 != nil {
			return ErrInternalDBError
		}

		if dbSize > 0 {
			err2 = tx.Descend(idxById, func(key, value string) bool {
				id, _ = strconv.Atoi(value)
				return false
			})
			if err2 != nil {
				return ErrInternalDBError
			}
		}

		// Increment to get the next id
		id++
		idStr = strconv.Itoa(id)

		if err2 = i.createIndexes(tx, idStr); err2 != nil {
			return err2
		}

		return i.storeIndexes(tx, idStr)
	})

	if err != nil {
		return err
	}

	// Store in memory
	i.db.loadIndexFields(i.definition)

	// Configure the field indexes
	return i.db.createIndexFields(idStr, i.definition.Fields)
}

func (i *Index) Definition() *api.IndexDefinition {
	return i.definition
}

// Delete first uses Truncate to clear segment then deletes the index
func (i *Index) Delete() error {
	if err := i.Truncate(); err != nil {
		return err
	}

	err := i.db.engine.Update(func(tx *buntdb.Tx) error {
		// Find the integer index of the index
		idx, err := tx.Get(idxKey(idxById, i.definition.Name), true)
		if err != nil {
			return ErrInternalDBError
		}

		if err = i.deleteKeys(idx, tx); err != nil {
			return err
		}

		return i.dropIndexes(idx, tx)
	})

	if err != nil {
		return err
	}

	return nil
}

func (i *Index) Proto() *api.IndexDefinition {
	return i.definition
}

func (i *Index) deleteKeys(idx string, tx *buntdb.Tx) error {
	keys := []string{
		idxKey(idxById, i.definition.Name),
		idxKey(fieldDefByIdx, i.definition.Name),
		idxKey(idxByString, idx),
	}

	for _, key := range keys {
		if _, err := tx.Delete(key); err != nil {
			return ErrInternalDBError
		}
	}

	return nil
}

func (i *Index) dropIndexes(idx string, tx *buntdb.Tx) error {
	indexes := []string{
		idx,
		idxKey(segmentByPrimaryKey, idx),
	}

	for _, field := range i.definition.Fields {
		indexes = append(indexes, idxKey(idx, field.Name))
	}

	for _, index := range indexes {
		if err := tx.DropIndex(index); err != nil {
			return ErrInternalDBError
		}
	}

	return nil
}

// Truncate deletes all segments from the index
func (i *Index) Truncate() error {
	err := i.db.engine.Update(func(tx *buntdb.Tx) error {
		// Find the integer index of the index
		idx, err := tx.Get(idxKey(idxById, i.definition.Name), true)
		if err != nil {
			return ErrInternalDBError
		}

		keysToDelete := make([]string, 0)

		err = tx.Ascend(idx, func(key, value string) bool {
			keysToDelete = append(keysToDelete, key)
			return true
		})

		if err != nil {
			return ErrInternalDBError
		}

		err = tx.Ascend(idxKey(segmentByPrimaryKey, idx), func(key, value string) bool {
			keysToDelete = append(keysToDelete, key)
			return true
		})

		if err != nil {
			return ErrInternalDBError
		}

		for _, k := range keysToDelete {
			if _, err = tx.Delete(k); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return ErrInternalDBError
	}

	return nil
}

// createIndexes ensures the segment indexes are created
func (i *Index) createIndexes(tx *buntdb.Tx, idStr string) error {
	// Create an Index patterns to use later for truncating and deleting segments
	err := tx.CreateIndex(idStr, idxKey(idStr, wildcard), buntdb.IndexString)
	if err != nil {
		return ErrInternalDBError
	}
	err = tx.CreateIndex(idxKey(segmentByPrimaryKey, idStr), idxKey(segmentByPrimaryKey, idStr, wildcard),
		buntdb.IndexString)
	if err != nil {
		return ErrInternalDBError
	}

	return nil
}

// storeIndexes is used to set internal indexes for the index
func (i *Index) storeIndexes(tx *buntdb.Tx, idStr string) error {
	// Store the index name by id
	_, replaced, err := tx.Set(idxKey(idxByString, idStr), i.definition.Name, nil)
	if err != nil {
		return ErrInternalDBError
	}
	if replaced {
		return ErrIndexExists
	}

	// Store the index id by name
	_, _, err = tx.Set(idxKey(idxById, i.definition.Name), idStr, nil)
	if err != nil {
		return ErrInternalDBError
	}

	// Store the definitions for cold starts
	_, _, err = tx.Set(idxKey(fieldDefByIdx, i.definition.Name), proto.MarshalTextString(i.definition),
		nil)
	if err != nil {
		return ErrInternalDBError
	}

	return nil
}

// loadIndexes is used to load all known indexes into memory, usually when starting the engine
func (db *DB) loadIndexes() error {
	err := db.engine.View(func(tx *buntdb.Tx) error {
		return tx.Ascend(fieldDefByIdx, func(name, index string) bool {
			indexProto := &api.IndexDefinition{}
			err := proto.UnmarshalText(index, indexProto)
			if err != nil {
				return false
			}

			db.loadIndexFields(indexProto)
			return true
		})
	})
	if err != nil {
		return ErrInternalDBError
	}
	return nil
}

// loadIndexFields is used to load all known fields into memory, usually when starting the engine
func (db *DB) loadIndexFields(index *api.IndexDefinition) {
	db.idx[index.Name] = index
	fields := make(map[string]*api.FieldDefinition, 0)

	for _, field := range index.Fields {
		fields[field.Name] = field
	}

	db.fields[index.Name] = fields
}

// createIndexFields registers all field indexes in the engine
func (db *DB) createIndexFields(path string, fields []*api.FieldDefinition) error {
	for _, field := range fields {
		err := db.createIndexField(path, field)
		if err != nil {
			return err
		}
	}
	return nil
}

// createIndexField prepares the correct indexes for a given field and key path (index)
func (db *DB) createIndexField(path string, field *api.FieldDefinition) (err error) {
	if field == nil {
		return ErrFieldUnknown
	}

	// Create indexes for each field
	name := idxKey(path, field.Name)

	switch field.DataType.(type) {
	case *api.FieldDefinition_Scalar:
		index, ok := fieldMapScalar[field.GetScalar()]
		if !ok {
			return ErrUnknownDataType
		}
		err = db.engine.CreateIndex(name, idxKey(name, wildcard), index)
	case *api.FieldDefinition_Geo:
		index, ok := fieldMapGeo[field.GetGeo()]
		if !ok {
			return ErrUnknownDataType
		}
		err = db.engine.CreateSpatialIndex(name, idxKey(name, wildcard), index)
	default:
		return ErrUnknownDataType
	}
	if err != nil {
		return ErrInternalDBError
	}

	for _, nestedField := range field.Fields {
		err = db.createIndexField(name, nestedField)
		if err != nil {
			return err
		}
	}

	return err
}

func (i *Index) UnmarshallPrimaryValue(value string) (*api.SegmentField, error) {
	var primaryDefinition *api.FieldDefinition
	for _, field := range i.definition.Fields {
		if field.IsPrimary {
			primaryDefinition = field
		}
	}

	if primaryDefinition == nil {
		return nil, ErrPrimaryKeyMissing
	}

	s := NewFieldDefinitionStringer(primaryDefinition)
	return s.UnmarshallText(value)
}
