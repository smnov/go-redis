package rdb

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

func DecodeRDBFile() (string, error) {
	rdbData, err := os.ReadFile("app/storage/db.rdb")
	if err != nil {
		return "", err
	}
	modifiedString := strings.ReplaceAll(string(rdbData), "\n", "")
	modifiedString = strings.ReplaceAll(modifiedString, "\r", "")

	fmt.Printf("modifiedString: %v\n", string(modifiedString))

	decodedString, err := hex.DecodeString(modifiedString)
	if err != nil {
		return "", err
	}

	return string(decodedString), nil
}
