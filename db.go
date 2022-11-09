package db

import (
	"context"
	"github.com/segmentq/protos-api-go"
	"github.com/tidwall/buntdb"
	"strings"
)

const (
	wildcard            = "*"
	idxSep              = ":"
	idxByString         = "@"
	idxByStringPattern  = idxByString + idxSep + wildcard
	idxById             = "#"
	idxByIdPattern      = idxById + idxSep + wildcard
	fieldDefByIdx       = "%"
	segmentByPrimaryKey = "$"
)

type DB struct {
	ctx    context.Context
	engine *buntdb.DB
	idx    map[string]*api.IndexDefinition            // All index definitions in memory
	fields map[string]map[string]*api.FieldDefinition // Field definitions by index and field
}

type DurabilityProfile int

const (
	RAM      DurabilityProfile = 0
	FastSync DurabilityProfile = 1
	Disk     DurabilityProfile = 2
	InMemory string            = ":memory:"
)

type ClientConfig struct {
	path       string
	durability DurabilityProfile
}

var (
	configMap = map[DurabilityProfile]*buntdb.Config{
		RAM: {
			SyncPolicy:         buntdb.Never,
			AutoShrinkDisabled: true,
		},
		FastSync: {
			SyncPolicy:           buntdb.EverySecond,
			AutoShrinkPercentage: 100,
			AutoShrinkMinSize:    32 * 1024 * 1024,
		},
		Disk: {
			SyncPolicy:           buntdb.Always,
			AutoShrinkPercentage: 50,
			AutoShrinkMinSize:    32 * 1024 * 1024,
		},
	}
)

// NewDB creates a simple database in memory
func NewDB(ctx context.Context) (*DB, error) {
	return NewDBWithConfig(ctx, &ClientConfig{InMemory, RAM})
}

// NewDBWithConfig creates a database using a ClientConfig you specify
func NewDBWithConfig(ctx context.Context, config *ClientConfig) (*DB, error) {
	engine, err := buntdb.Open(config.path)
	if err != nil {
		return nil, ErrInternalDBError
	}

	err = engine.ReadConfig(configMap[config.durability])
	if err != nil {
		return nil, ErrInternalDBError
	}

	db := &DB{
		ctx:    ctx,
		engine: engine,
	}

	err = db.init()
	if err != nil {
		return nil, err
	}

	return db, nil
}

// init builds the database from a cold start, warming all indexes and local maps
func (db *DB) init() error {
	db.idx = make(map[string]*api.IndexDefinition, 0)
	db.fields = make(map[string]map[string]*api.FieldDefinition, 0)

	// Ensure we have the correct indexes to start the DB
	indexes, err := db.engine.Indexes()
	if err != nil {
		return ErrInternalDBError
	}

	for _, idx := range indexes {
		if idxKey(idxByString) == idx {
			// We have a pre-configured database
			return db.loadIndexes()
		}
	}

	// Create the int index for compacted keys
	err = db.engine.CreateIndex(idxByString, idxByStringPattern, buntdb.IndexInt)
	if err != nil {
		return ErrInternalDBError
	}

	// Create the usual string index
	err = db.engine.CreateIndex(idxById, idxByIdPattern, buntdb.IndexString)
	if err != nil {
		return ErrInternalDBError
	}

	return nil
}

type Key struct {
	parts     []string
	separator string
}

func (k *Key) String() string {
	return strings.Join(k.parts, k.separator)
}

func (k *Key) fromString(str string) {
	k.parts = make([]string, 0)
	k.parts = strings.Split(str, k.separator)
}

func (k *Key) IndexId() (string, bool) {
	if len(k.parts) > 0 {
		return k.parts[0], true
	}
	return "", false
}

// FieldNameAtIndex gets the field name from a key at the given index, root == 0
func (k *Key) FieldNameAtIndex(fieldIndex int) (string, bool) {
	// Account for the index part
	pos := fieldIndex + 1

	// Don't allow items past the end of the field parts
	if len(k.parts) >= (pos + 2) {
		return k.parts[pos], true
	}
	return "", false
}

// SegmentKey finds the value of the segments primary key
func (k *Key) SegmentKey() (string, bool) {
	if len(k.parts) > 0 {
		return k.parts[len(k.parts)-2], true
	}
	return "", false
}

// FieldValueIndex finds the value index of the segments field value
func (k *Key) FieldValueIndex() (string, bool) {
	if len(k.parts) > 0 {
		return k.parts[len(k.parts)-1], true
	}
	return "", false
}

// idxKey is a helper method to join key segments using the idxSep separator
func idxKey(str ...string) string {
	key := Key{parts: str, separator: idxSep}
	return key.String()
}

// splitKey explodes a key into it's component parts
func keyFromString(key string) Key {
	k := Key{separator: idxSep}
	k.fromString(key)

	return k
}
