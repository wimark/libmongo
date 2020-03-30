package libmongo

import (
	"fmt"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

const (
	mongoConnectionTimeout = 15 * time.Second
	ERROR_NOT_CONNECTED    = "DB is not connected"
	ERROR_NOT_VALID        = "Query is not valid"
)

type MongoDb struct {
	sess *mgo.Session
}

type M = bson.M
type D = bson.D

func NewConnection(dsn string) (*MongoDb, error) {
	var db = MongoDb{}
	return &db, db.Connect(dsn)
}

func NewConnectionWithTimeout(dsn string, timeout time.Duration) (*MongoDb, error) {
	var db = MongoDb{}
	return &db, db.ConnectWithTimeout(dsn, timeout)
}

func (db *MongoDb) IsConnected() bool {
	return db.sess != nil
}

func (db *MongoDb) Connect(dsn string) error {
	var err error

	db.sess, err = mgo.DialWithTimeout(dsn, mongoConnectionTimeout)

	return err
}

func (db *MongoDb) ConnectWithTimeout(dsn string, timeout time.Duration) error {
	var err error

	if timeout < 1 {
		timeout = mongoConnectionTimeout
	}

	db.sess, err = mgo.DialWithTimeout(dsn, timeout)

	return err
}

func (db *MongoDb) Disconnect() {
	if db.IsConnected() {
		db.sess.Close()
	}
}

func (db *MongoDb) CreateIndexKey(coll string, key ...string) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).EnsureIndexKey(key...)

}

func (db *MongoDb) CreateIndexKeys(coll string, keys ...string) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var err error
	var sess = db.sess.Copy()
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
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Insert(v...)
}

func (db *MongoDb) InsertBulk(coll string, v ...interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	var err error
	var bulk = sess.DB("").C(coll).Bulk()
	bulk.Unordered()
	bulk.Insert(v...)
	_, err = bulk.Run()

	return err
}

func (db *MongoDb) InsertSess(coll string, sess *mgo.Session,
	v ...interface{}) error {
	if !db.IsConnected() || sess == nil {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}

	return sess.DB("").C(coll).Insert(v...)
}

func (db *MongoDb) Find(coll string, query map[string]interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	bsonQuery := bson.M{}

	for k, qv := range query {
		bsonQuery[k] = qv
	}

	return sess.DB("").C(coll).Find(bsonQuery).All(v)
}

func (db *MongoDb) Pipe(coll string, query []bson.M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Pipe(query).AllowDiskUse().All(v)
}

func (db *MongoDb) PipeOne(coll string, query []bson.M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Pipe(query).AllowDiskUse().One(v)
}

func (db *MongoDb) FindById(coll string, id string, v interface{}) bool {
	if !db.IsConnected() {
		return false
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return mgo.ErrNotFound != sess.DB("").C(coll).FindId(id).One(v)
}

func (db *MongoDb) FindAll(coll string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(bson.M{}).All(v)
}

func (db *MongoDb) FindWithQuery(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(query).One(v)
}

func (db *MongoDb) FindWithQuerySortOne(coll string, query interface{}, order string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(order).One(v)
}

func (db *MongoDb) FindWithQuerySortAll(coll string, query interface{}, order string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(order).All(v)
}

func (db *MongoDb) FindWithQuerySortLimitAll(coll string, query interface{}, order string, limit int, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(order).Limit(limit).All(v)
}

func (db *MongoDb) FindWithQueryOne(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(query).One(v)
}

func (db *MongoDb) FindWithQueryAll(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(query).All(v)
}

func (db *MongoDb) FindWithQuerySortLimitOffsetAll(coll string, query interface{}, sort string, limit int, offset int, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(sort).Limit(limit).Skip(offset).All(v)
}

func (db *MongoDb) FindWithQuerySortLimitOffsetTotalAll(coll string, query interface{},
	sort string, limit int, offset int, v interface{}, total *int) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	// find totatl
	*total, _ = sess.DB("").C(coll).Find(query).Count()

	return sess.DB("").C(coll).Find(query).Sort(sort).Limit(limit).Skip(offset).All(v)
}

func (db *MongoDb) Update(coll string, id interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Update(bson.M{"_id": id}, bson.M{"$set": v})
}

func (db *MongoDb) UpdateWithQuery(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Update(query, set)
}

func (db *MongoDb) UpdateWithQueryAll(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var err error
	var sess = db.sess.Copy()
	defer sess.Close()

	_, err = sess.DB("").C(coll).UpdateAll(query, set)

	return err
}

func (db *MongoDb) Upsert(coll string, id interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	var _, err = sess.DB("").C(coll).Upsert(bson.M{"_id": id}, v)

	return err
}

func (db *MongoDb) UpsertWithQuery(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var sess = db.sess.Copy()
	defer sess.Close()

	var _, err = sess.DB("").C(coll).Upsert(query, set)

	return err
}

func (db *MongoDb) UpsertMulti(coll string, id []interface{}, v []interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	if len(id) != len(v) {
		return fmt.Errorf("%s", ERROR_NOT_VALID)
	}
	var sess = db.sess.Copy()
	defer sess.Close()
	var index = 0
	for index < len(id) {
		sess.DB("").C(coll).Upsert(bson.M{"_id": id[index]}, v[index])
		index += 1
	}
	return nil
}

func (db *MongoDb) Remove(coll string, id interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
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
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
	}
	var err error
	var sess = db.sess.Copy()
	defer sess.Close()

	_, err = sess.DB("").C(coll).RemoveAll(query)

	return err
}

func (db *MongoDb) RemoveWithIDs(coll string, ids interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", ERROR_NOT_CONNECTED)
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

func GetDb() *MongoDb { return &MongoDb{} }
