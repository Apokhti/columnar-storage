package utils

import (
	"os"
	"path/filepath"
)

func DirPresent(makeIfNotPresent bool) (bool, error) {
	return false, nil
}

// Opens file and in case it does not exist and
func CreateFileRecursively(filePath string) (*os.File, error) {
	dirPath := filepath.Dir(filePath)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return os.Create(filePath)
}

// Decides if column with given name exists
func FileExists(columnPath string) bool {
	if _, err := os.Stat(columnPath); os.IsNotExist(err) {
		return false
	}
	return true
}

// SetUnion unions
func SetUnion(s1 map[int64]bool, s2 map[int64]bool) map[int64]bool {
	s_union := map[int64]bool{}
	for k, _ := range s1 {
		s_union[k] = true
	}
	for k, _ := range s2 {
		s_union[k] = true
	}
	return s_union
}

func SetIntersection(s1 map[int64]bool, s2 map[int64]bool) map[int64]bool {
	s_intersection := map[int64]bool{}
	if len(s1) > len(s2) {
		s1, s2 = s2, s1 // better to iterate over a shorter set
	}
	for k, _ := range s1 {
		if s2[k] {
			s_intersection[k] = true
		}
	}
	return s_intersection
}
