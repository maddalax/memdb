package db

type User struct {
	Id       string
	Username string
	Email    string
	Password string
}

func (u User) Key() string {
	return u.Id
}

var Users = CreateEntities[User]("./users")
