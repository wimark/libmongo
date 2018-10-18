package libmongo

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

const (
	mongoConnectionTimeout = 5 * time.Second
)

type MongoDb struct {
	sess *mgo.Session
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
	db.sess.Close()
}

func (db *MongoDb) CreateIndexKey(coll string, key ...string) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).EnsureIndexKey(key...)

}

func (db *MongoDb) CreateIndexKeys(coll string, keys ...string) error {
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
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Insert(v...)
}

func (db *MongoDb) Find(coll string, query map[string]interface{}, v interface{}) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	bsonQuery := bson.M{}

	for k, qv := range query {
		bsonQuery[k] = qv
	}

	return sess.DB("").C(coll).Find(bsonQuery).All(v)
}

func (db *MongoDb) Pipe(coll string, query []interface{}, v interface{}) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Pipe(query).AllowDiskUse().All(v)
}

func (db *MongoDb) FindById(coll string, id string, v interface{}) bool {
	var sess = db.sess.Copy()
	defer sess.Close()

	return mgo.ErrNotFound != sess.DB("").C(coll).FindId(id).One(v)
}

func (db *MongoDb) FindAll(coll string, v interface{}) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(bson.M{}).All(v)
}

func (db *MongoDb) FindWithQuery(coll string, query interface{}, v interface{}) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(query).One(v)
}

func (db *MongoDb) FindWithQueryAll(coll string, query interface{}, v interface{}) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(query).All(v)
}

func (db *MongoDb) Update(coll string, id interface{}, v interface{}) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Update(bson.M{"_id": id}, bson.M{"$set": v})
}

func (db *MongoDb) UpdateWithQuery(coll string, query interface{}, set interface{}) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Update(query, set)
}

func (db *MongoDb) UpdateWithQueryAll(coll string, query interface{}, set interface{}) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Update(query, set)
}

func (db *MongoDb) Upsert(coll string, id interface{}, v interface{}) error {
	var sess = db.sess.Copy()
	defer sess.Close()

	var _, err = sess.DB("").C(coll).Upsert(bson.M{"_id": id}, v)

	return err
}

func (db *MongoDb) Remove(coll string, id interface{}) error {
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
	var err error
	var sess = db.sess.Copy()
	defer sess.Close()

	_, err = sess.DB("").C(coll).RemoveAll(query)

	return err
}

func (db *MongoDb) SessExec(cb func(*mgo.Session)) {
	var sess = db.sess.Copy()
	defer sess.Close()

	cb(sess)
}

func GetDb() *MongoDb { return &MongoDb{} }
