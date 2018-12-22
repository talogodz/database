package database

import (
	"errors"
	"log"
	"reflect"

	"golang.org/x/net/context"

	"cloud.google.com/go/datastore"
)

type Database struct {
	Ctx    context.Context
	Client *datastore.Client
}

var DB *Database

func (db *Database) Init(projID string) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, projID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	db = &Database{ctx, client}
	DB = db
}

func (db *Database) PutIfNoSuchEntity(key *datastore.Key, v interface{}) error {
	_, err := db.Client.RunInTransaction(db.Ctx,
		func(tx *datastore.Transaction) error {
			err := tx.Get(key, v)
			if err == datastore.ErrNoSuchEntity {
				_, err = tx.Put(key, v)
			} else if err == nil {
				return errors.New("entity is already exists")
			}
			return err
		})
	return err
}

func (db *Database) Put(key *datastore.Key, v interface{}) error {
	_, err := db.Client.RunInTransaction(db.Ctx, func(tx *datastore.Transaction) error {
		_, err := db.Client.Put(db.Ctx, key, v)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (db *Database) GetAll(query *datastore.Query, dst interface{}) ([]*datastore.Key, error) {
	keys, err := db.Client.GetAll(db.Ctx, query, dst)
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func (db *Database) Update(key *datastore.Key, v interface{}) error {
	_, err := db.Client.RunInTransaction(db.Ctx,
		func(tx *datastore.Transaction) error {
			temp := reflect.New(reflect.ValueOf(v).Elem().Type()).Interface()
			if err := tx.Get(key, temp); err != nil {
				return err
			}
			log.Printf("value = %v", v)
			_, err := tx.Put(key, v)
			return err
		})

	if err != nil {
		return err
	}
	return nil
}

func GetDB() *Database {
	return DB
}
