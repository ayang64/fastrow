package fastrow

import (
	"database/sql"
	"reflect"
)

/*
	func (v Value) Elem() - if v is a pointer or interface, it returns the value that v points to.
	func (t Type) Elem() - returns the type data associated with t
*/

func Marshal(x interface{}, rows *sql.Rows) (interface{}, error) {
	/* extract type information from x */
	t := reflect.TypeOf(x)

	/* determine exactly the kind of symbol t is.  right now we only handle
	 * slices -- specifically slices of structs. */
	switch t.Kind() {
	case reflect.Slice:
		colmap := make(map[string]int)
		cols, err := rows.Columns()

		if err != nil {
			return x, err
		}

		/* store column indices for easy retrieval. */
		for idx, col := range cols {
			colmap[col] = idx
		}

		/* retrieve the type that TypeOf(x) points to.  So if reflect.TypeOf(x) is
		 * s slice, then reflect.TypeOf(x).Elem() will give us what x is a slice
		 * of. */
		te := t.Elem()

		/* get the value of x.  we'll be appendingn to this */
		v := reflect.ValueOf(x)

		/* walk our list of sql results. */
		for rows.Next() {
			nv := reflect.New(te)
			destslice := []interface{}{}

			/* rows.Scan() takes a variable number of interface{} values that contain
			 * pointers to elements to pack.  we can supply that by giving
			 * rows.Scan() a slice of interfaces{} and using the elipsis operator. */
			for _, col := range cols {
				for i := 0; i < te.NumField(); i++ {
					if f := te.Field(i); col == f.Tag.Get("col") {
						destslice = append(destslice, nv.Elem().Field(i).Addr().Interface())
						break
					}
				}
			}

			if err := rows.Scan(destslice...); err != nil {
				return x, err
			}

			v = reflect.Append(v, nv.Elem())
		}
		return v.Interface(), nil
	}

	return x, nil
}
