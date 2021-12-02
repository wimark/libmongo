package libmongo

import (
	"context"
	"reflect"

	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	ErrClientDisconnect = errors.New("client is disconnected")
	ErrNotFound         = errors.New("document is not found")
	ErrInterfaceSlice   = errors.New("interface is not slice")
	ErrInterfaceIsNil   = errors.New("interface is nil")
)

// Mongo - обёртка над клиентом MongoDB
type Mongo struct {
	client   *mongo.Client
	dbName   string
	readPref *readpref.ReadPref
}

type Operation func(ctx context.Context) error
type DecodeDocFunc func(m bson.M) error
type CursorIterFunc func(cursor *mongo.Cursor) error

// NewMongo - получение сущности клиента MongoDB
func NewMongo(ctx context.Context, opts *MongoOptions) (*Mongo, error) {
	cli, err := mongo.NewClient(opts.options)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = cli.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &Mongo{client: cli, dbName: opts.dbName, readPref: opts.readPref}, nil
}

// InsertOne - вставка документа
func (m Mongo) InsertOne(ctx context.Context, collection string, doc interface{}) error {
	if !m.isConnect(ctx) {
		return ErrClientDisconnect
	}
	coll := m.getCollection(collection)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// InsertMany - вставка документов
func (m Mongo) InsertMany(ctx context.Context, collection string, value interface{}) error {
	if !m.isConnect(ctx) {
		return ErrClientDisconnect
	}

	doc, err := m.toSlice(value)
	if err != nil {
		return err
	}

	coll := m.getCollection(collection)
	_, err = coll.InsertMany(ctx, doc)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// FindOne - поиск документа с декодирование в переменную `value`
func (m Mongo) FindOne(ctx context.Context, collection string, filter interface{}, decoded DecodeDocFunc) (err error) {
	if !m.isConnect(ctx) {
		return ErrClientDisconnect
	}
	var res bson.M
	err = m.getCollection(collection).FindOne(ctx, filter).Decode(&res)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return ErrNotFound
	} else if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(decoded(res))
}

// Find - поиск документов с возвратом курсора для его обхода
func (m Mongo) Find(ctx context.Context, collection string, filter interface{}, iterFunc CursorIterFunc) error {
	if !m.isConnect(ctx) {
		return ErrClientDisconnect
	}
	cursor, err := m.getCollection(collection).Find(ctx, filter)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(iterFunc(cursor))
}

// DeleteOne - удаление документа по заданному фильтру
func (m Mongo) DeleteOne(ctx context.Context, collection string, filter interface{}) error {
	if !m.isConnect(ctx) {
		return ErrClientDisconnect
	}
	_, err := m.getCollection(collection).DeleteOne(ctx, filter)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Aggregate - аггрегация данных в кастомной функцией итерации
func (m Mongo) Aggregate(ctx context.Context, collection string, pipeline Pipeline, iterFunc CursorIterFunc) error {
	if !m.isConnect(ctx) {
		return ErrClientDisconnect
	}
	cursor, err := m.getCollection(collection).Aggregate(ctx, pipeline)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(iterFunc(cursor))
}

func (m Mongo) AggregateAll(ctx context.Context, collection string, pipeline Pipeline, result interface{}) error {
	if !m.isConnect(ctx) {
		return ErrClientDisconnect
	}

	if !m.isSlice(result) {
		return ErrInterfaceSlice
	}

	cursor, err := m.getCollection(collection).Aggregate(ctx, pipeline)
	if err != nil {
		return errors.WithStack(err)
	}

	if err = cursor.All(ctx, result); err != nil {
		return errors.WithStack(err)
	}

	if err = cursor.Err(); err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(cursor.Close(ctx))

}

func (m Mongo) isConnect(ctx context.Context) bool {
	return m.client.Ping(ctx, nil) == nil
}

func (m Mongo) getCollection(collection string) *mongo.Collection {
	return m.client.Database(m.dbName).Collection(collection)
}
func (m Mongo) toSlice(value interface{}) ([]interface{}, error) {
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

func (m Mongo) isSlice(value interface{}) bool {
	if value == nil {
		return false
	}
	s := reflect.ValueOf(value)
	if s.Kind() != reflect.Ptr {
		return false
	}
	return s.Elem().Kind() == reflect.Slice
}

// SetUri - установка параметров соединения
func SetUri(uri string) MongoOption {
	return Options(func(options MongoOptions) MongoOptions {
		options.options.ApplyURI(uri)
		return options
	})
}

// SetTimeout - установка времени подключения
func SetTimeout(d time.Duration) MongoOption {
	return Options(func(options MongoOptions) MongoOptions {
		options.options.SetConnectTimeout(d)
		return options
	})
}

// SetMaxPoolSize - установка макскимального размера пула для конкуретной работы
func SetMaxPoolSize(u uint64) MongoOption {
	return Options(func(options MongoOptions) MongoOptions {
		options.options.SetMaxPoolSize(u)
		return options
	})
}

// SetDBName - установка имени базы данных
func SetDBName(name string) MongoOption {
	return Options(func(options MongoOptions) MongoOptions {
		options.dbName = name
		return options
	})
}

// SetPreferred - установка предпочтения операции чтения
// по-умолчанию выставляется режим secondaryPreferred
// В большинстве случаев операции читаются из вторичных элементов,
// но если вторичные члены недоступны, операции читаются из первичных в сегментированных кластерах.
func SetPreferred(mode readpref.Mode) MongoOption {
	return Options(func(options MongoOptions) MongoOptions {
		if mode.IsValid() {
			pref, err := readpref.New(mode)
			if err == nil {
				options.options.SetReadPreference(pref)
				options.readPref = pref
			}
		}
		return options
	})
}
