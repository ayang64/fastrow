package main

import (
	"database/sql"
	"fmt"
	"github.com/ayang64/fastrow"
	_ "github.com/lib/pq"
	"log"
)

type User struct {
	Id     int     `col:"id"`
	Name   string  `col:"name"`
	Salary float64 `col:"salary"`
}

type Department struct {
	Id   int    `col:"id"`
	Name string `col:"name"`
}

func (d Department) String() string {
	return fmt.Sprintf("{Id: %d, Name: %q}", d.Id, d.Name)
}

func (u User) String() string {
	return fmt.Sprintf("{Id: %d, Name: %q, Salary: %f}", u.Id, u.Name, u.Salary)
}

func GetUsers(db *sql.DB) ([]User, error) {
	rows, err := db.Query("select id, name, salary::numeric(10,2) from salary")
	if err != nil {
		log.Fatalf("couldn't connect to db: %s", err)
	}

	ifs, err := fastrow.Marshal([]User{}, rows)

	if err != nil {
		return nil, err
	}

	return ifs.([]User), nil
}

func GetDepartments(db *sql.DB) ([]Department, error) {
	rows, err := db.Query("select id, name from department")
	if err != nil {
		log.Fatalf("couldn't connect to db: %s", err)
	}

	ifs, err := fastrow.Marshal([]Department{}, rows)

	if err != nil {
		return nil, err
	}

	return ifs.([]Department), nil
}

func main() {
	db, err := sql.Open("postgres", "user=ayan dbname=ayan host=/var/run/postgresql")
	if err != nil {
		log.Fatalf("couldn't connect to db: %s", err)
	}

	users, err := GetUsers(db)

	for _, i := range users {
		log.Printf("%s", i)
	}

	deps, err := GetDepartments(db)
	for _, i := range deps {
		log.Printf("%s", i)
	}
}
