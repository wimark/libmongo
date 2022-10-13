package libmongo

import (
	"fmt"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

const (
	mongoConnectionTimeout = 15 * time.Second
	mongoQueryTimeout      = 30 * time.Second // default maxTimeMS timeout for Query
	errorNotConnected      = "DB is not connected"
	errorNotValid          = "Query is not valid"
)

type MongoDb struct {
	sync.RWMutex

	sess      *mgo.Session
	maxTimeMS time.Duration
}

type M = bson.M
type D = bson.D

type ChangeInfo *mgo.ChangeInfo

func NewConnection(dsn string) (*MongoDb, error) {
	var db = MongoDb{
		maxTimeMS: mongoQueryTimeout,
	}
	return &db, db.Connect(dsn)
}

func NewConnectionWithTimeout(dsn string, timeout time.Duration) (*MongoDb, error) {
	var db = MongoDb{
		maxTimeMS: mongoQueryTimeout,
	}
	return &db, db.ConnectWithTimeout(dsn, timeout)
}

func (db *MongoDb) IsConnected() bool {
	return db.sess != nil
}

func (db *MongoDb) Connect(dsn string) error {
	var err error

	db.sess, err = mgo.DialWithTimeout(dsn, mongoConnectionTimeout)
	db.sess.SetSafe(&mgo.Safe{WMode: "majority"})
	return err
}

func (db *MongoDb) ConnectWithTimeout(dsn string, timeout time.Duration) error {
	var err error

	if timeout < time.Second {
		timeout = mongoConnectionTimeout
	}

	db.sess, err = mgo.DialWithTimeout(dsn, timeout)
	db.sess.SetSafe(&mgo.Safe{WMode: "majority"})
	return err
}

func (db *MongoDb) SetMaxTimeMS(d time.Duration) {
	db.RWMutex.Lock()
	db.maxTimeMS = d
	db.RWMutex.Unlock()
}

func (db *MongoDb) Disconnect() {
	if db.IsConnected() {
		db.sess.Close()
	}
}

func (db *MongoDb) CreateIndexKey(coll string, key ...string) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).EnsureIndexKey(key...)
}

func (db *MongoDb) CreateIndexKeys(coll string, keys ...string) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var (
		err  error
		sess = db.sess.Copy()
	)

	defer sess.Close()

	for _, key := range keys {
		err = sess.DB("").C(coll).EnsureIndexKey(key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *MongoDb) Insert(coll string, v ...interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Insert(v...)
}

func (db *MongoDb) InsertWithCheckIsDup(coll string, v ...interface{}) (bool, error) {
	if !db.IsConnected() {
		return false, fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	err := sess.DB("").C(coll).Insert(v...)
	return mgo.IsDup(err), err
}

func (db *MongoDb) InsertBulk(coll string, v ...interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	var (
		err  error
		bulk = sess.DB("").C(coll).Bulk()
	)

	bulk.Unordered()
	bulk.Insert(v...)
	_, err = bulk.Run()

	return err
}

func (db *MongoDb) InsertSess(coll string, sess *mgo.Session,
	v ...interface{}) error {
	if !db.IsConnected() || sess == nil {
		return fmt.Errorf("%s", errorNotConnected)
	}

	return sess.DB("").C(coll).Insert(v...)
}

func (db *MongoDb) Find(coll string, query map[string]interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	var bsonQuery = bson.M{}

	for k, qv := range query {
		bsonQuery[k] = qv
	}

	return sess.DB("").C(coll).Find(bsonQuery).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *MongoDb) Pipe(coll string, query []bson.M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Pipe(query).AllowDiskUse().SetMaxTime(db.maxTimeMS).All(v)
}

func (db *MongoDb) PipeWithMaxTime(coll string, query []bson.M, v interface{}, maxTime time.Duration) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()
	sess.SetCursorTimeout(maxTime)
	return sess.DB("").C(coll).Pipe(query).AllowDiskUse().All(v)
}

func (db *MongoDb) PipeOne(coll string, query []bson.M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Pipe(query).AllowDiskUse().SetMaxTime(db.maxTimeMS).One(v)
}

func (db *MongoDb) FindByID(coll string, id string, v interface{}) bool {
	if !db.IsConnected() {
		return false
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return mgo.ErrNotFound != sess.DB("").C(coll).FindId(id).SetMaxTime(db.maxTimeMS).One(v)
}

func (db *MongoDb) FindAll(coll string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(bson.M{}).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *MongoDb) FindWithQuery(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).One(v)
}

func (db *MongoDb) FindWithQuerySortOne(coll string, query interface{},
	order string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(order).SetMaxTime(db.maxTimeMS).One(v)
}

func (db *MongoDb) FindWithQuerySortAll(coll string, query interface{},
	order string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(order).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *MongoDb) FindWithQuerySortLimitAll(coll string, query interface{},
	order string, limit int, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(order).Limit(limit).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *MongoDb) FindWithQueryOne(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).One(v)
}

func (db *MongoDb) FindWithQueryAll(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *MongoDb) FindWithQuerySortLimitOffsetAll(coll string, query interface{}, sort string,
	limit int, offset int, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(sort).Limit(limit).Skip(offset).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *MongoDb) FindWithQuerySortLimitOffsetTotalAll(coll string, query interface{},
	sort string, limit int, offset int, v interface{}, total *int) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	if total != nil {
		*total, _ = sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).Count()
	}

	return sess.DB("").C(coll).Find(query).Sort(sort).Limit(limit).Skip(offset).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *MongoDb) Count(coll string, query interface{}) (int, error) {
	if !db.IsConnected() {
		return 0, fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).Count()
}

func (db *MongoDb) Update(coll string, id interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Update(bson.M{"_id": id}, bson.M{"$set": v})
}

func (db *MongoDb) UpdateWithQuery(coll string, query interface{}, set interface{}, opts ...func(session *mgo.Session)) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()
	for _, opt := range opts {
		opt(sess)
	}

	defer sess.Close()

	return sess.DB("").C(coll).Update(query, set)
}

func (db *MongoDb) UpdateWithQueryAll(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var (
		err  error
		sess = db.sess.Copy()
	)

	defer sess.Close()

	_, err = sess.DB("").C(coll).UpdateAll(query, set)

	return err
}

func (db *MongoDb) Upsert(coll string, id interface{}, v interface{}) (ChangeInfo, error) {
	if !db.IsConnected() {
		return nil, fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Upsert(bson.M{"_id": id}, v)
}

func (db *MongoDb) UpsertWithQuery(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	var _, err = sess.DB("").C(coll).Upsert(query, set)

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
		sess  = db.sess.Copy()
	)

	defer sess.Close()

	for index < len(id) {
		// TODO: fix errcheck linter issue: return value is not checked
		sess.DB("").C(coll).Upsert(bson.M{"_id": id[index]}, v[index])
		index++
	}

	return nil
}

func (db *MongoDb) Remove(coll string, id interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	_, err := sess.DB("").C(coll).RemoveAll(bson.M{"_id": id})

	return err
}

func (db *MongoDb) RemoveAll(coll string) error {
	var sess = db.sess.Copy()

	defer sess.Close()

	_, err := sess.DB("").C(coll).RemoveAll(bson.M{})

	return err
}

func (db *MongoDb) RemoveWithQuery(coll string, query interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var (
		err  error
		sess = db.sess.Copy()
	)

	defer sess.Close()

	_, err = sess.DB("").C(coll).RemoveAll(query)

	return err
}

func (db *MongoDb) RemoveWithIDs(coll string, ids interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	_, err := sess.DB("").C(coll).RemoveAll(bson.M{"_id": bson.M{"$in": ids}})

	return err
}

func (db *MongoDb) SessExec(cb func(*mgo.Session)) {
	if !db.IsConnected() {
		return
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	cb(sess)
}

func (db *MongoDb) SessCopy() *mgo.Session {
	if !db.IsConnected() {
		return nil
	}

	return db.sess.Copy()
}

func (db *MongoDb) SessClose(sess *mgo.Session) {
	if !db.IsConnected() || sess == nil {
		return
	}

	sess.Close()
}

func (db *MongoDb) Run(dbname string, cmd bson.D, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB(dbname).Run(cmd, set)
}

func (db *MongoDb) CollectionNames() (names []string, err error) {
	if !db.IsConnected() {
		return nil, fmt.Errorf("%s", errorNotConnected)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").CollectionNames()
}

func (db *MongoDb) Iter(coll string, query []bson.M, f func(iter *mgo.Iter) error) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	iter := sess.DB("").C(coll).Pipe(query).Iter()
	return f(iter)
}

func GetDb() *MongoDb { return &MongoDb{} }
