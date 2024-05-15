// Package resp provides parsers that determine type of given data append
// parse them by RESP (Redis serialization protocol specification) protocol.
package resp

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	Array      = '*'
	BulkString = '$'
	Quote      = '"'
)

// Parse string
func ParseRequestString(received string) ([]string, error) {
	typ := received[0]
	fmt.Printf("typ: %v\n", string(typ))
	switch typ {
	case Array:
		return parseArray(received[1:])
	case BulkString:
		return parseBulkString(received)
	case Quote:
		return ParseRequestString(received[1:])
	default:
		res := strings.Split(received, " ")
		return res, nil

	}
}

// If given request is an array, this function parses it
func parseArray(input string) ([]string, error) {
	var result []string
	parts := strings.Split(input, "\r\n")
	for i := 0; i < len(parts); i++ {
		if strings.HasPrefix(parts[i], "$") {
			length, err := strconv.Atoi(parts[i][1:])
			if err != nil {
				return nil, err
			}
			if i+1 < len(parts) {
				result = append(result, parts[i+1][:length])
				i++
			} else {
				break
			}
		}
	}
	return result, nil
}

func parseBulkString(input string) ([]string, error) {
	var result []string
	parts := strings.Split(input, "\r\n")
	for i := 0; i < len(parts); i++ {
		if strings.HasPrefix(parts[i], "$") {
			length, err := strconv.Atoi(parts[i][1:])
			if err != nil {
				return nil, err
			}
			if i+1 < len(parts) {
				result = append(result, parts[i+1][:length])
				i++
			} else {
				break
			}
		}
	}
	return result, nil
}

func CreateArray(input []string) string {
	result := "*" + strconv.Itoa(len(input)) + "\r\n"
	for _, v := range input {
		decodedString := "$" + strconv.Itoa(len(v)) + "\r\n" + v + "\r\n"
		result += decodedString
	}
	return result
}

// Wrap a string array so it comforms to RESP protocol
func CreateBulkStringFromArray(input []string) string {
	var res string
	for _, s := range input {
		res += "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"
	}
	return res
}

// Wrap a string so it comforms to RESP protocol
func CreateBulkString(input string) string {
	return "$" + strconv.Itoa(len(input)) + "\r\n" + input + "\r\n"
}
