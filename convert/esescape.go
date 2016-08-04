package convert

import "strings"

// ESEscape escapes out "." characters which are illegal in fieldnames
// in es 2.x
// This turns existing "_" characters into double "__" characters, and then
// turns "." characters into "_" characters
func ESEscape(field string) string {
	field = strings.Replace(field, "_", "__", -1)
	field = strings.Replace(field, ".", "_", -1)
	return field
}
