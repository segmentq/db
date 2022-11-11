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
	return index, index.Create()
}

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

func newIndex(db *DB, definition *api.IndexDefinition) *Index {
	return &Index{
		db:         db,
		definition: definition,
	}
}

// Create is used when the Index is instantiated directly
func (i *Index) Create() error {
	var idStr string
	err := i.db.engine.Update(func(tx *buntdb.Tx) error {
		// Determine the last insert id
		id := 0
		dbSize, err := tx.Len()
		if err != nil {
			return ErrInternalDBError
		}

		if dbSize > 0 {
			err = tx.Descend(idxByStringPattern, func(key, value string) bool {
				id, _ = strconv.Atoi(value)
				return false
			})
			if err != nil {
				return ErrInternalDBError
			}
		}

		// Increment to get the next id
		id++
		idStr = strconv.Itoa(id)

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

func (db *DB) createIndexField(path string, field *api.FieldDefinition) (err error) {
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
