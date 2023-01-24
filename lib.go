package libmongo

import (
	"context"
	"fmt"
	"sync"
	"time"

	// "github.com/globalsign/mgo/bson"
	"github.com/qiniu/qmgo"

	"go.mongodb.org/mongo-driver/mongo"
)

const (
	mongoConnectionTimeout = 15 * time.Second
	mongoQueryTimeout      = 30 * time.Second // default maxTimeMS timeout for Query
	errorNotConnected      = "DB is not connected"
	errorNotValid          = "Query is not valid"
)

type MongoDb struct {
	sync.RWMutex

	client    *qmgo.Client
	database  string
	maxTimeMS time.Duration
}

type (
	UpdateResult = qmgo.UpdateResult
	IndexModel   = mongo.IndexModel
)

var (
	ErrNoSuchDocuments = qmgo.ErrNoSuchDocuments
)

func NewConnection(uri, database string) (*MongoDb, error) {
	if database == "" {
		database = "test"
	}
	db := MongoDb{
		maxTimeMS: mongoQueryTimeout,
		database:  database,
	}
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db.client, err = qmgo.NewClient(ctx, &qmgo.Config{Uri: uri})
	return &db, err
}

func NewConnectionWithTimeout(uri, database string, timeout time.Duration) (*MongoDb, error) {
	if database == "" {
		database = "test"
	}
	var db = MongoDb{
		maxTimeMS: mongoQueryTimeout,
		database:  database,
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tm := int64(timeout)
	var err error
	db.client, err = qmgo.NewClient(ctx, &qmgo.Config{Uri: uri, ConnectTimeoutMS: &tm})
	return &db, err
}

func (db *MongoDb) IsConnected() bool {
	return db.client != nil
}

func (db *MongoDb) SetMaxTimeMS(d time.Duration) {
	db.RWMutex.Lock()
	db.maxTimeMS = d
	db.RWMutex.Unlock()
}

func (db *MongoDb) Disconnect() {
	if db.IsConnected() {
		if err := db.client.Close(context.Background()); err != nil {
			panic(err)
		}
	}
}

func (db *MongoDb) Insert(coll string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	_, err := db.client.Database(db.database).Collection(coll).InsertOne(ctx, v)
	return err
}

func (db *MongoDb) InsertMany(coll string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	_, err := db.client.Database(db.database).Collection(coll).InsertMany(ctx, v)
	return err
}

func (db *MongoDb) InsertBulk(coll string, v ...interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	b := db.client.Database(db.database).Collection(coll).Bulk()
	b.SetOrdered(false)
	for _, vv := range v {
		b.InsertOne(vv)
	}
	_, err := b.Run(ctx)

	return err
}

func (db *MongoDb) Find(coll string, query map[string]interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	var bsonQuery = M{}
	for k, qv := range query {
		bsonQuery[k] = qv
	}

	return db.client.Database(db.database).Collection(coll).Find(ctx, bsonQuery).All(v)
}

func (db *MongoDb) Pipe(coll string, query []M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	return db.client.Database(db.database).Collection(coll).Aggregate(ctx, query).All(v)
}

func (db *MongoDb) PipeOne(coll string, query []M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	return db.client.Database(db.database).Collection(coll).Aggregate(ctx, query).One(v)
}

func (db *MongoDb) FindByID(coll string, id string, v interface{}) bool {
	if !db.IsConnected() {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	return qmgo.ErrNoSuchDocuments != db.client.Database(db.database).Collection(coll).Find(ctx, M{"_id:": id}).One(v)
}

func (db *MongoDb) FindAll(coll string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	return db.client.Database(db.database).Collection(coll).Find(ctx, M{}).All(v)
}

func (db *MongoDb) FindWithSelectAll(coll string, query, sel, output interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	return db.client.Database(db.database).Collection(coll).Find(ctx, query).Select(sel).All(output)
}

func (db *MongoDb) FindWithQuery(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	return db.client.Database(db.database).Collection(coll).Find(ctx, query).One(v)
}

func (db *MongoDb) FindWithQuerySortOne(coll string, query interface{},
	order string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	return db.client.Database(db.database).Collection(coll).Find(ctx, query).Sort(order).One(v)
}

func (db *MongoDb) FindWithQuerySortAll(coll string, query interface{},
	order string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	return db.client.Database(db.database).Collection(coll).Find(ctx, query).Sort(order).All(v)
}

func (db *MongoDb) FindWithQuerySortLimitAll(coll string, query interface{},
	order string, limit int64, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	return db.client.Database(db.database).Collection(coll).Find(ctx, query).Sort(order).Limit(limit).All(v)
}

func (db *MongoDb) FindWithQueryOne(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	return db.client.Database(db.database).Collection(coll).Find(ctx, query).One(v)
}

func (db *MongoDb) FindWithQueryAll(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	return db.client.Database(db.database).Collection(coll).Find(ctx, query).All(v)
}

func (db *MongoDb) FindWithQuerySortLimitOffsetAll(coll string, query interface{}, sort string,
	limit int64, offset int64, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	return db.client.Database(db.database).Collection(coll).Find(ctx, query).Sort(sort).Limit(limit).Skip(offset).All(v)
}

func (db *MongoDb) FindWithQuerySortLimitOffsetTotalAll(coll string, query interface{},
	sort string, limit int64, offset int64, v interface{}, total *int64) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS*2)
	defer cancel()
	if total != nil {
		*total, _ = db.client.Database(db.database).Collection(coll).Find(ctx, query).Count()
	}

	return db.client.Database(db.database).Collection(coll).Find(ctx, query).Sort(sort).Limit(limit).Skip(offset).All(v)
}

func (db *MongoDb) Count(coll string, query interface{}) (int64, error) {
	if !db.IsConnected() {
		return 0, fmt.Errorf("%s", errorNotConnected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	return db.client.Database(db.database).Collection(coll).Find(ctx, query).Count()
}

func (db *MongoDb) Update(coll string, id interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	return db.client.Database(db.database).Collection(coll).UpdateOne(ctx, M{"_id": id}, M{"$set": v})
}

func (db *MongoDb) UpdateWithQuery(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	// var sess = db.sess.Copy()
	// for _, opt := range opts {
	// 	opt(sess)
	// }

	// defer sess.Close()
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	_, err := db.client.Database(db.database).Collection(coll).UpdateAll(ctx, query, set)
	return err
}

func (db *MongoDb) UpdateWithQueryAll(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	// var (
	// 	err  error
	// 	sess = db.sess.Copy()
	// )

	// defer sess.Close()

	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()

	_, err := db.client.Database(db.database).Collection(coll).UpdateAll(ctx, query, set)

	return err
}

func (db *MongoDb) Upsert(coll string, id interface{}, v interface{}) (*UpdateResult, error) {
	if !db.IsConnected() {
		return nil, fmt.Errorf("%s", errorNotConnected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	return db.client.Database(db.database).Collection(coll).Upsert(ctx, M{"_id": id}, v)
}

func (db *MongoDb) UpsertWithQuery(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	var _, err = db.client.Database(db.database).Collection(coll).Upsert(ctx, query, set)

	return err
}

func (db *MongoDb) UpsertMulti(coll string, id []interface{}, v []interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	if len(id) != len(v) {
		return fmt.Errorf("%s", errorNotValid)
	}

	var (
		index = 0
	)
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	for index < len(id) {
		// TODO: fix errcheck linter issue: return value is not checked
		db.client.Database(db.database).Collection(coll).Upsert(ctx, M{"_id": id[index]}, v[index])
		index++
	}

	return nil
}

func (db *MongoDb) Remove(coll string, id interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	_, err := db.client.Database(db.database).Collection(coll).RemoveAll(context.Background(), M{"_id": id})

	return err
}

func (db *MongoDb) RemoveAll(coll string) error {
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	_, err := db.client.Database(db.database).Collection(coll).RemoveAll(ctx, M{})

	return err
}

func (db *MongoDb) RemoveWithQuery(coll string, query interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	_, err := db.client.Database(db.database).Collection(coll).RemoveAll(ctx, query)

	return err
}

func (db *MongoDb) RemoveWithIDs(coll string, ids interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), db.maxTimeMS)
	defer cancel()
	_, err := db.client.Database(db.database).Collection(coll).RemoveAll(ctx, M{"_id": M{"$in": ids}})

	return err
}

// func (db *MongoDb) SessExec(cb func(*mgo.Session)) {
// 	if !db.IsConnected() {
// 		return
// 	}

// 	var sess = db.sess.Copy()

// 	defer sess.Close()

// 	cb(sess)
// }

// func (db *MongoDb) Run(dbname string, cmd D, set interface{}) error {
// 	if !db.IsConnected() {
// 		return fmt.Errorf("%s", errorNotConnected)
// 	}
// 	var sess = db.sess.Copy()
// 	defer sess.Close()

// 	return sess.DB(dbname).Run(cmd, set)
// }

// func (db *MongoDb) CollectionNames() (names []string, err error) {
// 	if !db.IsConnected() {
// 		return nil, fmt.Errorf("%s", errorNotConnected)
// 	}
// 	var sess = db.sess.Copy()
// 	defer sess.Close()

// 	return sess.DB("").CollectionNames()
// }

// func (db *MongoDb) Iter(coll string, query []M, f func(iter *mgo.Iter) error) error {
// 	if !db.IsConnected() {
// 		return fmt.Errorf("%s", errorNotConnected)
// 	}
// 	var sess = db.sess.Copy()
// 	defer sess.Close()

// 	iter := db.client.Database(db.database).Collection(coll).Pipe(query).Iter()
// 	return f(iter)
// }

func GetDb() *MongoDb { return &MongoDb{} }
