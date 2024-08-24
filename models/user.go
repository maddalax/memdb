package models

import (
	"memdb/db"
	"strings"
)

type User struct {
	Id       string
	Username string
	Email    string
	Password string
}

type Id struct {
	Id string
}

func (u User) Key() string {
	return u.Id
}

func (i Id) Key() string {
	return i.Id
}

var Users = db.CreateEntitiesWithHooks[User]("./users.json", db.Hooks[User]{
	OnSet: func(key string, value User) {

	},
	OnRemove: func(key string, value User) {

	},
})

var UsersByEmail = db.CreateIndex(Users, func(user User) string {
	return strings.Split(user.Email, "@")[1]
})

var Books = db.CreateEntities[User]("./books.json")
