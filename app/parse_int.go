package main

import (
	"encoding/binary"
	"io"
	"log"
)

func parseUInt8(stream io.Reader) uint8 {
	var result uint8

	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		if err == io.EOF {
			log.Fatalf("Unexpected EOF when reading uint8 - likely reading past page boundary")
		}
		log.Fatalf("Error when reading uint8: %v", err)
	}

	return result
}

func parseUInt16(stream io.Reader) uint16 {
	var result uint16

	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		if err == io.EOF {
			log.Fatalf("Unexpected EOF when reading uint16 - likely reading past page boundary")
		}
		log.Fatalf("Error when reading uint16: %v", err)
	}

	return result
}

func parseUInt24(stream io.Reader) uint32 {
	bytes := make([]byte, 3)
	n, err := stream.Read(bytes)
	if err != nil {
		if err == io.EOF {
			log.Fatalf("Unexpected EOF when reading uint24 (read %d/3 bytes) - likely reading past page boundary", n)
		}
		log.Fatalf("Error when reading uint24: %v", err)
	}
	if n != 3 {
		log.Fatalf("Short read when reading uint24: expected 3 bytes, got %d", n)
	}
	return uint32(bytes[0])<<16 | uint32(bytes[1])<<8 | uint32(bytes[2])
}

func parseUInt32(stream io.Reader) uint32 {
	var result uint32
	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		if err == io.EOF {
			log.Fatalf("Unexpected EOF when reading uint32 - likely reading past page boundary")
		}
		log.Fatalf("Error when reading uint32: %v", err)
	}
	return result
}

func parseUInt48(stream io.Reader) uint64 {
	bytes := make([]byte, 6)
	n, err := stream.Read(bytes)
	if err != nil {
		if err == io.EOF {
			log.Fatalf("Unexpected EOF when reading uint48 (read %d/6 bytes) - likely reading past page boundary", n)
		}
		log.Fatalf("Error when reading uint48: %v", err)
	}
	if n != 6 {
		log.Fatalf("Short read when reading uint48: expected 6 bytes, got %d", n)
	}
	return uint64(bytes[0])<<40 | uint64(bytes[1])<<32 | uint64(bytes[2])<<24 |
		uint64(bytes[3])<<16 | uint64(bytes[4])<<8 | uint64(bytes[5])
}

func parseUInt64(stream io.Reader) uint64 {
	var result uint64
	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		if err == io.EOF {
			log.Fatalf("Unexpected EOF when reading uint64 - likely reading past page boundary")
		}
		log.Fatalf("Error when reading uint64: %v", err)
	}
	return result
}

func parseFloat64(stream io.Reader) float64 {
	var result float64
	if err := binary.Read(stream, binary.BigEndian, &result); err != nil {
		if err == io.EOF {
			log.Fatalf("Unexpected EOF when reading float64 - likely reading past page boundary")
		}
		log.Fatalf("Error when reading float64: %v", err)
	}
	return result
}
