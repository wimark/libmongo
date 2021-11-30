package libmongo

import (
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoOption - абстракция сущности установки опций
type MongoOption interface {
	apply(MongoOptions) MongoOptions
}

// Combine - приминение установленных опций
func Combine(opts ...MongoOption) *MongoOptions {
	clientOpts := newOptions()
	for i := len(opts) - 1; i >= 0; i-- {
		opts[i].apply(clientOpts)
	}
	return &clientOpts
}

// newOptions - создание обёртки опций подключения
// "test" - имя БД по-умолчанию
func newOptions() MongoOptions {
	return MongoOptions{
		options: options.Client().SetReadPreference(readpref.Secondary()),
		dbName:  "test",
	}
}

// MongoOptions - обёртка над опциями подключения
type MongoOptions struct {
	options *options.ClientOptions
	dbName  string
}

// Options - тип функции применяющий опции к подключению
type Options func(MongoOptions) MongoOptions

func (opt Options) apply(options MongoOptions) MongoOptions {
	return opt(options)
}
