package models

import "memdb/db"

type User struct {
	Id       string
	Username string
	Email    string
	Password string
}

func (u User) Key() string {
	return u.Id
}

var Users = db.CreateEntities[User]("./users")
