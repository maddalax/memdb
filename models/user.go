package models

import (
	"memdb/db"
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
		//if strings.Contains(strings.ToLower(value.Email), "gmail") {
		//	UsersWithGmail.Add(Id{Id: key})
		//}
	},
	OnRemove: func(key string, value User) {
		//if strings.Contains(strings.ToLower(value.Email), "gmail") {
		//	UsersWithGmail.Remove(Id{Id: key})
		//}
	},
})

var UsersWithGmail = db.CreateEntities[Id]("./users_with_gmail.json")

var Books = db.CreateEntities[User]("./books.json")
