package fastrow

import (
	"database/sql"
	_ "github.com/lib/pq"
	"testing"
)

func TestFastrow(t *testing.T) {
	createTempTable := func(db *sql.DB) func() {
		// create temporary table
		dml := `
drop table if exists temptest;
create table temptest (
	id			serial,
	name		text,
	age			integer
);

insert into temptest (id, name, age) values
	(default, 'Bob',		40),
	(default, 'Jane',		35),
	(default, 'Steve',	37),
	(default, 'Hank',		25),
	(default, 'Jamal',	32),
	(default, 'Sara',		10);
`
		if _, err := db.Exec(dml); err != nil {
			t.Fatal(err)
			t.FailNow()
		}

		return func() {
			t.Logf("dropping temp table.")
			db.Exec(`drop table temptest;`)
		}
	}

	type Employee struct {
		ID   int    `col:"id"`
		Name string `col:"name"`
		Age  int    `col:"age"`
	}

	db, err := sql.Open("postgres", "user=ayan host=/tmp database=ayan")

	if err != nil {
		t.Fatalf("sql.Open() failed: %v", err)
		t.FailNow()
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("sql.Open() failed: %v", err)
		t.FailNow()
	}

	defer createTempTable(db)()

	rows, err := db.Query(`select id, name, age from temptest;`)

	if err != nil {
		t.Fatal(err)
		t.FailNow()
	}

	e := []Employee{}
	if err := Marshal(&e, rows); err != nil {
		t.Fatal(err)
		t.FailNow()
	}

	t.Logf("results: %#v", e)

	d := (*DB)(db)

	e = []Employee{}
	d.Query(&e, "select * from temptest where age=32;")

	t.Logf("results: %#v", e)
}
