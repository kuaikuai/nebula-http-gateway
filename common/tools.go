package common

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"strconv"
)

func CreateFileWithPerm(filePath string, permCode string) (*os.File, error) {

	if abs := filepath.IsAbs(filePath); !abs {
		return nil, errors.New("file path should be absolute path")
	}

	perm, err := strconv.ParseInt(permCode, 8, 64)
	if err != nil {
		return nil, err
	}
	// Set directory permission directly
	filedir := path.Dir(filePath)
	os.MkdirAll(filedir, os.FileMode(perm))
	fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(perm))
	defer fd.Close()
	if os.IsExist(err) {
		os.Chmod(filePath, os.FileMode(perm))
	}
	return fd, err
}
