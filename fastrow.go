package fastrow

import (
	"database/sql"
	"fmt"
	"reflect"
)

// Expects a slice of structs.
func Marshal(x interface{}, rows *sql.Rows) (interface{}, error) {
	// extract type information stored in x interface
	t := reflect.TypeOf(x)

	// determine exactly the kind of symbol t is.  right now we only handle
	// slices -- specifically slices of structs. */

	if t.Kind() != reflect.Slice {
		return x, fmt.Errorf("Marshal requires a slice of structs.")
	}

	cols, err := rows.Columns()

	if err != nil {
		return x, err
	}

	// retrieve the type that TypeOf(x) points to.  So if reflect.TypeOf(x) is
	// s slice, then reflect.TypeOf(x).Elem() will give us what x is a slice
	// of.
	te := t.Elem()

	// get the value of x.  we'll be appending to this
	v := reflect.ValueOf(x)

	// store column indices for easy retrieval.
	colmap := make(map[string]int)
	for idx, col := range cols {
		colmap[col] = idx
	}

	// store field indices in the order that they're needed for retrieval.
	var fields []int
	for i := 0; i < te.NumField(); i++ {
		if col, ok := te.Field(i).Tag.Lookup("col"); ok {
			fields = append(fields, colmap[col])
		}
	}

	destslice := make([]interface{}, len(fields), len(fields))

	// walk our list of sql results.
	nv := reflect.New(te)

	for idx, field := range fields {
		destslice[idx] = nv.Elem().Field(field).Addr().Interface()
	}

	for rows.Next() {
		// rows.Scan() takes a variable number of interface{} values that contain
		// pointers to elements to pack.  we can supply that by giving
		// rows.Scan() a slice of interfaces{} and using the elipsis operator.
		if err := rows.Scan(destslice...); err != nil {
			return x, err
		}

		v = reflect.Append(v, nv.Elem())
	}
	return v.Interface(), nil
}
