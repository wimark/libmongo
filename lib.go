package libmongo

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	mongoConnectionTimeout = 15 * time.Second
	mongoQueryTimeout      = 30 * time.Second // default maxTimeMS timeout for Query
	errorNotConnected      = "DB is not connected"
	errorNotValid          = "Query is not valid"
)

type MongoDb struct {
	sync.RWMutex

	ctx       context.Context
	dbName    string
	client    *mongo.Client
	maxTimeMS time.Duration
}

type M = bson.M
type D = bson.D

func NewConnection(dsn string, mode readpref.Mode) (*MongoDb, error) {
	ctx := context.Background()
	mongoOptions := Combine(SetUri(dsn), SetPreferred(mode), SetTimeout(mongoQueryTimeout), SetMaxPoolSize(20))
	client, err := mongo.Connect(ctx, mongoOptions.ClientOptions())
	if err != nil {
		return nil, err
	}
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return &MongoDb{
		ctx:       context.Background(),
		dbName:    mongoOptions.DBName(),
		client:    client,
		maxTimeMS: mongoQueryTimeout,
	}, nil
}

func (db *MongoDb) getCollection(coll string) *mongo.Collection {
	return db.client.Database(db.dbName).Collection(coll)
}

func NewConnectionWithTimeout(dsn string, timeout time.Duration, mode readpref.Mode) (*MongoDb, error) {
	var db = MongoDb{
		maxTimeMS: mongoQueryTimeout,
		ctx:       context.Background(),
	}
	return &db, db.ConnectWithTimeout(dsn, timeout, mode)
}

func (db *MongoDb) IsConnected() bool {
	return db.client.Ping(db.ctx, nil) != nil
}

func (db *MongoDb) Connect(_ string) error {
	return db.client.Connect(db.ctx)
}

func (db *MongoDb) ConnectWithTimeout(dsn string, timeout time.Duration, mode readpref.Mode) error {

	if timeout < time.Second {
		timeout = mongoConnectionTimeout
	}

	var err error
	mongoOptions := Combine(SetUri(dsn), SetTimeout(timeout), SetPreferred(mode), SetMaxPoolSize(20))
	db.client, err = mongo.Connect(db.ctx, mongoOptions.ClientOptions())
	if err != nil {
		return err
	}

	return db.client.Connect(db.ctx)
}

func (db *MongoDb) SetMaxTimeMS(d time.Duration) {
	db.RWMutex.Lock()
	db.maxTimeMS = d
	db.RWMutex.Unlock()
}

func (db *MongoDb) Disconnect() {
	if db.IsConnected() {
		db.client.Disconnect(db.ctx)
	}
}

func (db *MongoDb) CreateIndexKey(coll string, key ...string) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	var err error
	collection := db.getCollection(coll)
	for i := range key {
		indexModel := mongo.IndexModel{Keys: bson.D{{key[i], 1}}}
		_, err = collection.Indexes().CreateOne(db.ctx, indexModel)
		if err != nil {
			return err
		}
	}

	return err
}

func (db *MongoDb) CreateIndexKeys(coll string, keys ...string) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var err error
	collection := db.getCollection(coll)
	for i := range keys {
		indexModel := mongo.IndexModel{Keys: bson.D{{keys[i], 1}}}
		_, err = collection.Indexes().CreateOne(db.ctx, indexModel)
		if err != nil {
			return err
		}
	}

	return err
}

func (db *MongoDb) Insert(coll string, docs ...interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	docs, err := toSlice(docs)
	if err != nil {
		return err
	}

	_, err = db.getCollection(coll).InsertMany(db.ctx, docs)
	return err
}

func (db *MongoDb) InsertWithCheckIsDup(coll string, docs ...interface{}) (bool, error) {
	if !db.IsConnected() {
		return false, fmt.Errorf("%s", errorNotConnected)
	}
	err := db.Insert(coll, docs)
	return mongo.IsDuplicateKeyError(err), err
}

func (db *MongoDb) InsertBulk(coll string, docs ...interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var writeModels = make([]mongo.WriteModel, len(docs), len(docs))
	for i, doc := range docs {
		writeModels[i] = mongo.NewInsertOneModel().SetDocument(doc)
	}

	opts := options.BulkWrite().SetOrdered(false)
	_, err := db.getCollection(coll).BulkWrite(db.ctx, writeModels, opts)
	if err != nil {
		return err
	}
	return err
}

/*func (db *MongoDb) InsertSess(coll string, sess *mgo.Session,
	v ...interface{}) error {
	if !db.IsConnected() || sess == nil {
		return fmt.Errorf("%s", errorNotConnected)
	}

	return sess.DB("").C(coll).Insert(v...)
}*/

func (db *MongoDb) Find(coll string, query map[string]interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	filter := bson.D{{}}
	for key, value := range query {
		filter = append(filter, bson.E{Key: key, Value: value})
	}

	opts := options.Find().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true)
	cursor, err := collection.Find(db.ctx, filter, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return cursor.All(db.ctx, v)
}

func (db *MongoDb) Pipe(coll string, query []bson.M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	opts := options.Aggregate().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true)
	collection := db.getCollection(coll)
	cursor, err := collection.Aggregate(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return cursor.All(db.ctx, v)
}

func (db *MongoDb) PipeWithMaxTime(coll string, query []bson.M, v interface{}, maxTime time.Duration) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	opts := options.Aggregate().
		SetMaxTime(maxTime).
		SetAllowDiskUse(true)
	collection := db.getCollection(coll)
	cursor, err := collection.Aggregate(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return cursor.All(db.ctx, v)
}

func (db *MongoDb) PipeOne(coll string, query []bson.M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	opts := options.Aggregate().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true)
	collection := db.getCollection(coll)
	cursor, err := collection.Aggregate(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return bson.Unmarshal(cursor.Current, v)
}

func (db *MongoDb) FindByID(coll string, id string, v interface{}) bool {
	if !db.IsConnected() {
		return false
	}

	collection := db.getCollection(coll)
	opts := options.FindOne().SetMaxTime(db.maxTimeMS)
	res := collection.FindOne(db.ctx, bson.D{{"_id", id}}, opts)

	if mongo.ErrNoDocuments != res.Err() {
		if err := res.Decode(v); err != nil {
			return false
		}
		return true
	}

	return false
}

func (db *MongoDb) FindAll(coll string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Find().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true)

	cursor, err := collection.Find(db.ctx, bson.D{{}}, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return cursor.All(db.ctx, v)
}

func (db *MongoDb) FindWithQuery(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Find().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true)

	cursor, err := collection.Find(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)
	return cursor.All(db.ctx, v)
}

func (db *MongoDb) FindWithQuerySortOne(coll string, query interface{},
	order D, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Find().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true).
		SetSort(order)

	cursor, err := collection.Find(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return bson.Unmarshal(cursor.Current, v)
}

func (db *MongoDb) FindWithQuerySortAll(coll string, query interface{},
	order D, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Find().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true).SetSort(order)

	cursor, err := collection.Find(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return cursor.All(db.ctx, v)
}

func (db *MongoDb) FindWithQuerySortLimitAll(coll string, query interface{},
	order D, limit int, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Find().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true).
		SetSort(order).
		SetLimit(int64(limit))

	cursor, err := collection.Find(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return cursor.All(db.ctx, v)
}

func (db *MongoDb) FindWithQueryOne(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Find().SetMaxTime(db.maxTimeMS).SetAllowDiskUse(true)

	cursor, err := collection.Find(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return bson.Unmarshal(cursor.Current, v)
}

func (db *MongoDb) FindWithQueryAll(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Find().SetMaxTime(db.maxTimeMS).SetAllowDiskUse(true)

	cursor, err := collection.Find(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return cursor.All(db.ctx, v)
}

func (db *MongoDb) FindWithQuerySortLimitOffsetAll(coll string, query interface{}, sort D,
	limit int, offset int, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Find().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true).
		SetSort(sort).
		SetLimit(int64(limit)).SetSkip(int64(offset))

	cursor, err := collection.Find(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return cursor.All(db.ctx, v)
}

func (db *MongoDb) FindWithQuerySortLimitOffsetTotalAll(coll string, query interface{},
	sort string, limit int, offset int, v interface{}, total *int) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	if total != nil {
		opts := options.Count().SetMaxTime(db.maxTimeMS)
		count, err := collection.CountDocuments(db.ctx, query, opts)
		if err != nil {
			return err
		}
		*total = int(count)
	}

	opts := options.Find().
		SetMaxTime(db.maxTimeMS).
		SetAllowDiskUse(true).
		SetSort(sort).
		SetLimit(int64(limit)).SetSkip(int64(offset))

	cursor, err := collection.Find(db.ctx, query, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(db.ctx)

	return cursor.All(db.ctx, v)
}

func (db *MongoDb) Count(coll string, query interface{}) (int, error) {
	if !db.IsConnected() {
		return 0, fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Count().SetMaxTime(db.maxTimeMS)
	count, err := collection.CountDocuments(db.ctx, query, opts)
	if err != nil {
		return 0, err
	}

	return int(count), nil

}

func (db *MongoDb) Update(coll string, id interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	_, err := collection.UpdateByID(db.ctx, id, bson.M{"$set": v})
	return err

}

func (db *MongoDb) UpdateWithQuery(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	_, err := collection.UpdateOne(db.ctx, query, set)
	return err
}

func (db *MongoDb) UpdateWithQueryAll(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	_, err := collection.UpdateMany(db.ctx, query, set)

	return err
}

func (db *MongoDb) Upsert(coll string, id interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Update().SetUpsert(true)
	_, err := collection.UpdateByID(db.ctx, id, bson.M{"$set": v}, opts)
	return err
}

func (db *MongoDb) UpsertWithQuery(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	opts := options.Update().SetUpsert(true)
	_, err := collection.UpdateOne(db.ctx, query, set, opts)
	return err
}

func (db *MongoDb) UpsertMulti(coll string, id []interface{}, v []interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	if len(id) != len(v) {
		return fmt.Errorf("%s", errorNotValid)
	}

	var err error
	collection := db.getCollection(coll)
	opts := options.Update().SetUpsert(true)

	for i := 0; i < len(id); i++ {
		filter := bson.D{{"_id", id[i]}}
		update := bson.D{{"$set", v[i]}}
		_, err = collection.UpdateOne(db.ctx, filter, update, opts)
	}

	return err

}

func (db *MongoDb) Remove(coll string, id interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	_, err := collection.DeleteOne(db.ctx, id)
	return err
}

func (db *MongoDb) RemoveAll(coll string) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	_, err := collection.DeleteMany(db.ctx, bson.D{{}})
	return err
}

func (db *MongoDb) RemoveWithQuery(coll string, query interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	collection := db.getCollection(coll)
	_, err := collection.DeleteMany(db.ctx, query)
	return err
}

func (db *MongoDb) RemoveWithIDs(coll string, ids interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	filter := bson.D{{"_id", bson.D{{"$in", ids}}}}
	collection := db.getCollection(coll)
	_, err := collection.DeleteMany(db.ctx, filter)
	return err

}

/*func (db *MongoDb) SessExec(cb func(*mgo.Session)) {
	if !db.IsConnected() {
		return
	}

	var sess = db.client.Copy()

	defer sess.Close()

	cb(sess)
}*/

/*func (db *MongoDb) SessCopy() *mgo.Session {
	if !db.IsConnected() {
		return nil
	}

	return db.client.Copy()
}*/

/*func (db *MongoDb) SessClose(sess *mgo.Session) {
	if !db.IsConnected() || sess == nil {
		return
	}

	sess.Close()
}*/

/*func (db *MongoDb) Run(dbname string, cmd bson.D, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	db.client.Database(db.dbName).RunCommand(db.ctx, cmd)
	var sess = db.client.Copy()
	defer sess.Close()

	return sess.DB(dbname).Run(cmd, set)
}*/

func (db *MongoDb) CollectionNames() (names []string, err error) {
	if !db.IsConnected() {
		return nil, fmt.Errorf("%s", errorNotConnected)
	}
	cursor, err := db.client.Database(db.dbName).ListCollections(db.ctx, bson.D{{}})
	if err != nil {
		return nil, err
	}

	for cursor.Next(db.ctx) {
		var s string
		if err = cursor.Decode(&s); err != nil {
			return nil, err
		}
		names = append(names, s)
	}

	return
}

func (db *MongoDb) Iter(coll string, query []bson.M, f func(cursor *mongo.Cursor) error) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	collection := db.getCollection(coll)
	opts := options.Aggregate().SetMaxTime(db.maxTimeMS).SetAllowDiskUse(true)
	cursor, err := collection.Aggregate(db.ctx, query, opts)
	if err != nil {
		return err
	}
	return f(cursor)
}

func GetDb() *MongoDb { return &MongoDb{} }
func toSlice(value interface{}) ([]interface{}, error) {
	s := reflect.ValueOf(value)
	if s.Kind() != reflect.Slice {
		return nil, ErrInterfaceSlice
	}

	if s.IsNil() {
		return nil, ErrInterfaceIsNil
	}

	doc := make([]interface{}, s.Len(), s.Cap())
	for i := 0; i < s.Len(); i++ {
		doc[i] = s.Index(i).Interface()
	}
	return doc, nil
}
