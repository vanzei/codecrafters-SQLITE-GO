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

func parserRecordDynamic(stream io.Reader) Record {
	headerLen, consumed := parseVarintWithLen(stream)

	var serialTypes []int
	for consumed < int(headerLen) {
		serialType, n := parseVarintWithLen(stream)
		serialTypes = append(serialTypes, serialType)
		consumed += n
	}

	// Don't include the header length as a value - only parse the actual column values
	values := make([]interface{}, len(serialTypes))
	for i, serialType := range serialTypes {
		values[i] = parseRecordValue(stream, serialType)
	}
	return Record{values: values}
}

func parseRecordValue(stream io.Reader, serialType int) interface{} {
	switch serialType {
	case 0:
		// NULL value
		return nil
	case 1:
		// 8-bit twos-complement integer
		return parseUInt8(stream)
	case 2:
		// Big-endian 16-bit twos-complement integer
		return parseUInt16(stream)
	case 3:
		// Big-endian 24-bit twos-complement integer
		return parseUInt24(stream)
	case 4:
		// Big-endian 32-bit twos-complement integer
		return parseUInt32(stream)
	case 5:
		// Big-endian 48-bit twos-complement integer
		return parseUInt48(stream)
	case 6:
		// Big-endian 64-bit twos-complement integer
		return parseUInt64(stream)
	case 7:
		// Big-endian IEEE 754-2008 64-bit floating point number
		return parseFloat64(stream)
	case 8:
		// Integer constant 0
		return uint64(0)
	case 9:
		// Integer constant 1
		return uint64(1)
	default:
		if serialType >= 12 && serialType%2 == 0 {
			// BLOB that is (N-12)/2 bytes in length
			bytesCount := (serialType - 12) / 2
			value := make([]byte, bytesCount)
			_, _ = stream.Read(value)
			return value
		} else if serialType >= 13 && serialType%2 == 1 {
			// Text encoding that is (N-13)/2 bytes in length
			bytesCount := (serialType - 13) / 2
			value := make([]byte, bytesCount)
			_, _ = stream.Read(value)
			return value
		} else {
			log.Fatalf("Unsupported serial type: %d", serialType)
			return nil
		}
	}
}
