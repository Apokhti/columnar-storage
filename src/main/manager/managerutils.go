package manager

import "strings"

func extractRecord(str string) string {
	return strings.Split(str, ")")[1]
}
