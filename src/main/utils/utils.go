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
