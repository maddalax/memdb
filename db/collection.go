package db

type User struct {
	Username string
	Email    string
	Password string
}

func (u User) Eq(other User) bool {
	return u.Username == other.Username
}

var Users = CreateEntities[User]("./users")
