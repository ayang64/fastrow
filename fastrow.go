package fastrow

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
)

type DB sql.DB

func (db *DB) Query(i interface{}, f string, args ...interface{}) error {
	d := (*sql.DB)(db)

	rows, err := d.Query(f, args...)

	if err != nil {
		return err
	}

	return Marshal(i, rows)
}

// Marshal expects a slice of structs.
func Marshal(w interface{}, rows *sql.Rows) error {
	deref := reflect.ValueOf(w)

	if deref.Kind() != reflect.Ptr {
		return fmt.Errorf("Marshal() requires a pointer to a slice of structs")
	}

	log.Printf("deref.Kind() = %v", deref.Kind())

	x := deref.Elem()
	log.Printf("x.Kind() = %v", x.Kind())

	// extract type information stored in x interface
	// t := reflect.TypeOf(x)
	t := x.Type()

	log.Printf("t.Kind() = %v", t.Kind())

	// determine exactly the kind of symbol t is.  right now we only handle
	// slices -- specifically slices of structs.
	if t.Kind() != reflect.Slice {
		return fmt.Errorf("Marshal() requires a pointer to a slice of structs; not a pointer to a slice of %v", t.Kind())
	}

	// Extract column data from our rows.  we're mainly interested in the number
	// and names of the columns.  database/sql.Rows.Columns() returns a slice of
	// strings containing column names in the order they were requested.
	cols, err := rows.Columns()

	if err != nil {
		return err
	}

	// Store column indices for easy retrieval.  This simply creates a map of
	// column names to thier numeric index.  This will allow us to lookup a
	// column's index by name.
	colmap := make(map[string]int)
	for idx, col := range cols {
		colmap[col] = idx
	}

	// Retrieve the type that TypeOf(x) points to.  So if reflect.TypeOf(x) is a
	// slice, then reflect.TypeOf(x).Elem() will give us what x is a slice of.
	//
	// Elem() works on composite types like maps, arrays, slices, channels, and
	// pointers.
	te := t.Elem()

	// Build an index of fields to field index. This requires us to iterate over
	// the fields in a struct and if that field has a tag that translates it to a
	// column in our result set, then store its index in the map.
	field := make(map[string]int)
	for i := 0; i < te.NumField(); i++ {
		if col, ok := te.Field(i).Tag.Lookup("col"); ok {
			if _, exists := colmap[col]; exists == false {
				continue
			}
			field[col] = i
		}
	}

	// The following is a simple sanity check.  If the number of fields that we're mapping
	// to doesn't equal the number of columns we're mapping from, then there's a
	// major problem.
	if len(field) != len(cols) {
		return fmt.Errorf("struct field/column count mismatch")
	}

	// At this point, we should have all of the information we need to build are
	// result set that we'll Scan() to.
	//
	// The database/sql.Rows.Scan() function takes a variable length list of
	// empty interfaces.  Here we build that list.
	destslice := make([]interface{}, len(cols), len(cols))

	// Allocate a new instance of the struct type that we're mapping to.  This is
	// the actual destination of the results we're scanning.
	nv := reflect.New(te)

	// For each of the columns where're mapping, find the corresponding field in
	// our type, obtain a new interface value for the fields address, and assign
	// that value to our destslice.  This effectively packs
	for idx, col := range cols {
		// The long form of this might be something like:

		// e := nv.Elem() // since nv i sa pointer to our struct, we have to use Elem() to retrieve an element of that type.
		//
		// f := e.Field(field[col]) // this retrieves the index for the field of the column that has the apropriate field tag for the column name.
		//
		// a := f.Addr() // retrieve the address of that field.
		//
		// i := a.Interface() // return an interface value for the address of the field.
		destslice[idx] = nv.Elem().Field(field[col]).Addr().Interface()
	}

	// At this point, destslice should be properly packed.

	// We will be appending new results to the slice that was passed to this
	// function.  We must retrive the actual ValueOf() the slice from the
	// interface that was wassed.  We can then append to this value.
	// v := reflect.ValueOf(x)

	// Iterate over result set.
	for rows.Next() {
		// rows.Scan() takes a variable number of interface{} values that contain
		// pointers to elements to pack.  Below, we satisfy this by giving
		// rows.Scan() a slice of interfaces{} and using the elipsis operator to
		// expand it into multiple arguments.
		//
		// The following command writes the results to our struct's membres.
		if err := rows.Scan(destslice...); err != nil {
			return err
		}

		// Append struct data to our resulting slice.
		x.Set(reflect.Append(x, nv.Elem()))
	}

	log.Printf("x.Len() = %d", x.Len())
	log.Printf("x.CanAddr() = %v", x.CanAddr())
	log.Printf("x.CanSet() = %v", x.CanSet())
	log.Printf("deref.CanSet() = %v", deref.CanSet())
	log.Printf("deref.CanAddr() = %v", deref.CanAddr())
	log.Printf("x.UnsafeAddr() = %v", x.UnsafeAddr())

	// Return the an interface value for our resulting slice.  The caller will
	// have to type assert it.
	///	deref.SetPointer(unsafe.Pointer(x.UnsafeAddr()))
	// deref.Set(x.Addr())
	return nil
}
