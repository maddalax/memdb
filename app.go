package main

import (
	"fmt"
	"github.com/labstack/echo/v4"
	"math"
	"memdb/db"
	"runtime"
	"strconv"
	"strings"
)

func main() {
	e := echo.New()

	// Routes
	e.GET("/users", func(c echo.Context) error {
		page, _ := strconv.Atoi(c.QueryParam("page"))
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
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
