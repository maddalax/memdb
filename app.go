package main

import (
	"fmt"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/labstack/echo/v4"
	"math"
	"memdb/db"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func main() {
	e := echo.New()

	e.GET("/insert", func(c echo.Context) error {
		for i := 0; i < 5_000_000; i++ {
			db.Users.Add(db.User{
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
		db.Users.RemoveBy(func(u db.User) bool {
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
		name := c.QueryParam("name")

		if name != "" {
			limit = math.MaxInt32
		}

		results := db.Users.RangeFilter(start, start+limit, func(u db.User) bool {
			if name != "" {
				return strings.Contains(u.Username, name)
			}
			return true
		})
		return c.JSON(200, results)
	})

	go func() {
		for {
			PrintMemUsage()
			time.Sleep(5 * time.Second)
		}
	}()

	e.Start(":8080")

}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
	all := db.Users.Filter(func(u db.User) bool {
		return true
	})
	fmt.Printf("Total users: %d\n", len(all))
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
