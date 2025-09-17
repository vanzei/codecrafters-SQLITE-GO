package main

import (
	"encoding/binary"
	"io"
	"log"
)

func parseUInt8(stream io.Reader) uint8 {
	var result uint8

	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatalf("Error when reading uint8: %v", err)
	}

	return result
}

func parseUInt16(stream io.Reader) uint16 {
	var result uint16

	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatalf("Error when reading uint8: %v", err)
	}

	return result
}

func parseUInt24(stream io.Reader) uint32 {
	bytes := make([]byte, 3)
	_, err := stream.Read(bytes)
	if err != nil {
		log.Fatalf("Error when reading uint24: %v", err)
	}
	return uint32(bytes[0])<<16 | uint32(bytes[1])<<8 | uint32(bytes[2])
}

func parseUInt32(stream io.Reader) uint32 {
	var result uint32
	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatalf("Error when reading uint32: %v", err)
	}
	return result
}

func parseUInt48(stream io.Reader) uint64 {
	bytes := make([]byte, 6)
	_, err := stream.Read(bytes)
	if err != nil {
		log.Fatalf("Error when reading uint48: %v", err)
	}
	return uint64(bytes[0])<<40 | uint64(bytes[1])<<32 | uint64(bytes[2])<<24 |
		uint64(bytes[3])<<16 | uint64(bytes[4])<<8 | uint64(bytes[5])
}

func parseUInt64(stream io.Reader) uint64 {
	var result uint64
	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatalf("Error when reading uint64: %v", err)
	}
	return result
}

func parseFloat64(stream io.Reader) float64 {
	var result float64
	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		log.Fatalf("Error when reading float64: %v", err)
	}
	return result
}
