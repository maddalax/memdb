package main

import (
	"fmt"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/labstack/echo/v4"
	"memdb/db"
	"memdb/models"
	"memdb/util"
	"strconv"
	"strings"
	"time"
)

func main() {
	e := echo.New()

	e.GET("/insert", func(c echo.Context) error {
		for i := 0; i < 1_000_000; i++ {
			if i%1_000_000 == 0 {
				fmt.Printf("Inserted %d users\n", i)
			}
			name := gofakeit.Username()
			email := gofakeit.Email()
			if gofakeit.Bool() {
				email = fmt.Sprintf("%s@%s", name, "gmail.com")
			}
			models.Users.Add(models.User{
				Id:       gofakeit.UUID(),
				Username: gofakeit.Username(),
				Email:    email,
				Password: "password",
			})
		}
		return c.NoContent(201)
	})

	e.GET("/perf", func(c echo.Context) error {
		email := c.QueryParam("email")
		count := 0
		util.TracePerf("count users via index", func() {
			for _, _ = range models.UsersByEmail.Each(email) {
				count++
			}
		})
		util.TracePerf("count users via each", func() {
			for u := range models.Users.Each() {
				if strings.HasSuffix(u.Email, email) {
					count++
				}
			}
		})
		return c.JSON(200, count)
	})

	e.GET("/delete", func(c echo.Context) error {
		name := c.QueryParam("name")
		models.Users.RemoveBy(func(u models.User) bool {
			return strings.Contains(strings.ToLower(u.Username), strings.ToLower(name)) || strings.Contains(strings.ToLower(u.Email), strings.ToLower(name))
		})
		return c.NoContent(201)
	})

	e.GET("/sydne", func(c echo.Context) error {
		return c.JSON(200, db.ToSlice(models.Users.FilterLimit(100, func(user models.User) bool {
			return true
		})))
	})

	// Routes
	e.GET("/users", func(c echo.Context) error {
		page, _ := strconv.Atoi(c.QueryParam("page"))
		if page > 0 {
			page = page - 1
		}
		limit, _ := strconv.Atoi(c.QueryParam("limit"))
		start := page * limit
		end := start + limit
		name := c.QueryParam("name")

		return c.JSON(200, db.ToSlice(models.Users.RangeFilter(start, end, func(user models.User) bool {
			return strings.Contains(user.Username, name)
		})))
	})

	go func() {
		for {
			models.Users.PrintMetrics()
			models.Books.PrintMetrics()
			time.Sleep(2 * time.Second)
		}
	}()

	db.WaitForLoad()
	err := e.Start(":8080")

	if err != nil {
		fmt.Println("Error starting server")
	}

}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
