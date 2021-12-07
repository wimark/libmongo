package libmongo

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const mongoUri = `mongodb://127.0.0.1:27017`

type Data struct {
	ID   string `json:"_id" bson:"_id"`
	Data int    `json:"data" bson:"data"`
}

func TestCrudOperationMongo(t *testing.T) {
	ctx := context.Background()
	client, err := NewMongo(ctx, Combine(SetUri(mongoUri), SetTimeout(20*time.Second),
		SetMaxPoolSize(20), SetPreferred(readpref.PrimaryMode)))

	require.Equal(t, nil, err)
	require.Equal(t, client != nil, true)

	d := Data{
		ID:   uuid.New().String(),
		Data: 1,
	}
	err = client.InsertOne(ctx, "coll", d)
	require.Equal(t, nil, err)

	filter := bson.D{{"_id", d.ID}}
	d2 := &Data{}
	err = client.FindOne(ctx, "coll", filter, unmarshal(d2))

	require.Equal(t, nil, err)
	require.Equal(t, d, *d2, "docs is not equal")

	err = client.DeleteOne(ctx, "coll", filter)
	require.Equal(t, nil, err)

	d3 := &Data{}
	err = client.FindOne(ctx, "coll", filter, unmarshal(d3))
	require.Equal(t, ErrNotFound, err)

	limit := 5
	var data = make(map[string]struct{})
	for i := 1; i <= limit; i++ {
		d = Data{
			ID:   uuid.New().String(),
			Data: i,
		}
		err = client.InsertOne(ctx, "coll", d)
		require.Equal(t, nil, err)
		data[d.ID] = struct{}{}
	}

	filter = bson.D{{"data", bson.D{{"$gte", 3}}}}

	err = client.Find(ctx, "coll", filter, func(cursor *mongo.Cursor) (err error) {
		defer func(cursor *mongo.Cursor, ctx context.Context) {
			err = cursor.Close(ctx)
			if err != nil {
				return
			}
		}(cursor, ctx)

		for cursor.Next(ctx) {
			d := Data{}
			err = cursor.Decode(&d)
			if err != nil {
				return err
			}
			_, ok := data[d.ID]
			err = client.DeleteOne(ctx, "coll", bson.D{{"_id", d.ID}})
			require.Equal(t, true, ok)
			require.Equal(t, nil, err)
		}

		return cursor.Err()
	})
	require.Equal(t, nil, err)

}
func TestInsertMany(t *testing.T) {
	ctx := context.Background()
	client, err := NewMongo(ctx, Combine(SetUri(mongoUri), SetTimeout(20*time.Second),
		SetMaxPoolSize(20), SetPreferred(readpref.PrimaryMode)))

	require.Equal(t, nil, err)
	require.Equal(t, client != nil, true)

	limit := 10
	data, keys := generateData(limit)

	err = client.InsertMany(ctx, "coll", data)
	require.Equal(t, nil, err)

	in := bson.D{{"$in", keys}}
	filter := bson.D{{"_id", in}}
	var result []Data
	err = client.Find(ctx, "coll", filter, func(cursor *mongo.Cursor) error {
		if err := cursor.All(ctx, &result); err != nil {
			return err
		}
		if err := cursor.Err(); err != nil {
			return err
		}

		return cursor.Close(ctx)
	})

	sort.Slice(result, func(i, j int) bool {
		return result[i].Data < result[j].Data
	})
	sort.Slice(data, func(i, j int) bool {
		return data[i].Data < data[j].Data
	})

	require.Equal(t, data, result)

}
func TestAggregate(t *testing.T) {
	ctx := context.Background()
	client, err := NewMongo(ctx, Combine(SetUri(mongoUri), SetTimeout(20*time.Second),
		SetMaxPoolSize(20), SetPreferred(readpref.PrimaryMode)))
	require.Equal(t, nil, err)
	require.Equal(t, client != nil, true)

	limit := 10
	data, _ := generateData(limit)

	err = client.InsertMany(ctx, "coll", data)
	require.Equal(t, nil, err)

	pipe := Pipeline{
		{
			"$match": bson.M{
				"data": bson.M{
					"$gte": 3,
				},
			},
		},
	}

	t.Run("Aggregate", func(t *testing.T) {
		var res []Data
		err = client.Aggregate(ctx, "coll", pipe, func(cursor *mongo.Cursor) error {
			for cursor.Next(ctx) {
				var d Data
				errDec := cursor.Decode(&d)
				if errDec != nil {
					return errDec
				}
				res = append(res, d)
			}
			if curErr := cursor.Err(); err != nil {
				return curErr
			}
			return cursor.Close(ctx)
		})
		require.Equal(t, nil, err)

		for _, r := range res {
			require.Equal(t, true, r.Data >= 3)
		}
	})

	t.Run("AggregateAll", func(t *testing.T) {
		var res []Data
		err = client.AggregateAll(ctx, "coll", pipe, &res)
		require.Equal(t, nil, err)

		for _, r := range res {
			require.Equal(t, true, r.Data >= 3)
		}
	})

	t.Run("AggregateWithError", func(t *testing.T) {
		for _, i := range []interface{}{[]Data{}, Data{}, &Data{}, nil} {
			err = client.AggregateAll(ctx, "coll", pipe, i)
			require.Equal(t, ErrInterfaceSlice, err)
		}
	})

}
func TestUpdate(t *testing.T) {
	ctx := context.Background()
	client, err := NewMongo(ctx, Combine(SetUri(mongoUri), SetTimeout(20*time.Second),
		SetMaxPoolSize(20), SetPreferred(readpref.PrimaryMode)))
	require.Equal(t, nil, err)
	require.Equal(t, client != nil, true)

	limit := 5
	data, keys := generateData(limit)

	err = client.InsertMany(ctx, "coll", data)
	require.Equal(t, nil, err)

	id := keys[0]
	filter := bson.D{{"_id", id}}
	update := bson.D{{"$set", bson.D{{"data", 111}}}}

	err = client.UpdateMany(ctx, "coll", filter, update, false)
	require.Equal(t, nil, err)

	var result = &Data{}
	err = client.FindOne(ctx, "coll", bson.D{{"_id", id}}, unmarshal(&result))
	require.Equal(t, err, nil)
	require.Equal(t, &Data{ID: id, Data: 111}, result)

	update = bson.D{{"$set", bson.D{{"data", 222}}}}
	err = client.UpdateOne(ctx, "coll", filter, update, false)

	err = client.FindOne(ctx, "coll", bson.D{{"_id", id}}, unmarshal(&result))
	require.Equal(t, err, nil)
	require.Equal(t, &Data{ID: id, Data: 222}, result)

	update = bson.D{{"$set", bson.D{{"data", 333}}}}
	err = client.UpdateByID(ctx, "coll", id, update, false)

	err = client.FindOne(ctx, "coll", bson.D{{"_id", id}}, unmarshal(&result))
	require.Equal(t, err, nil)
	require.Equal(t, &Data{ID: id, Data: 333}, result)
}

func generateData(limit int) ([]Data, []string) {
	var data []Data
	var keys []string
	for i := 1; i <= limit; i++ {
		id := uuid.New().String()
		data = append(data, Data{
			ID:   id,
			Data: i,
		})
		keys = append(keys, id)
	}
	return data, keys
}
func unmarshal(i interface{}) func(m bson.M) error {
	return func(m bson.M) error {
		b, err := bson.Marshal(m)
		if err != nil {
			return err
		}
		return bson.Unmarshal(b, i)
	}
}
