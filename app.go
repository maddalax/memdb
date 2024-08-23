package main

import (
	"fmt"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/labstack/echo/v4"
	"memdb/db"
	"memdb/models"
	"runtime"
	"strconv"
	"strings"
)

func main() {
	e := echo.New()

	e.GET("/insert", func(c echo.Context) error {
		for i := 0; i < 1_000_000; i++ {
			if i%1_000_000 == 0 {
				fmt.Printf("Inserted %d users\n", i)
			}
			models.Users.Add(models.User{
				Id:       gofakeit.UUID(),
				Username: gofakeit.Username(),
				Email:    "",
				Password: "password",
			})
		}
		return c.NoContent(201)
	})

	e.GET("/delete", func(c echo.Context) error {
		name := c.QueryParam("name")
		models.Users.RemoveBy(func(u models.User) bool {
			return strings.Contains(u.Username, name)
		})
		return c.NoContent(201)
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

	//go func() {
	//	for {
	//		PrintMemUsage()
	//		time.Sleep(1 * time.Second)
	//	}
	//}()

	err := e.Start(":8080")

	if err != nil {
		fmt.Println("Error starting server")
	}

}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
	count := 0
	for _ = range models.Users.Each() {
		count++
	}
	fmt.Printf("Total users: %d\n", count)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
