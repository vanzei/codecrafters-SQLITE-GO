package main

import (
	"io"
	"log"
)

type Record struct {
	values []interface{}
}

func parserRecord(stream io.Reader, valuesCount int) Record {
	parseVarint(stream)

	serialTypes := make([]int, valuesCount)
	for i := 0; i < valuesCount; i++ {
		serialTypes[i] = parseVarint(stream)
	}

	values := make([]interface{}, valuesCount)
	for i, serialType := range serialTypes {
		values[i] = parseRecordValue(stream, serialType)
	}
	return Record{values: values}
}

func parseRecordValue(stream io.Reader, serialType int) interface{} {
	if serialType >= 13 && serialType%2 == 1 {
		// text encoding
		bytesCount := (serialType - 13) / 2
		value := make([]byte, bytesCount)
		_, _ = stream.Read(value)
		return value
	} else if serialType == 1 {
		return parseUInt8(stream)
	} else {
		log.Fatalf("Unsupported serial type: %d", serialType)
		return nil
	}

}
