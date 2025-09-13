package main

import (
	"os"
)

type PageHearder struct {
	pageType    uint8
	firstFree   uint16
	numCells    uint16
	startOfCell uint16
	fragmented  uint8
}

func parserHeader(databaseFile *os.File) PageHearder {

	pageType := parseUInt8(databaseFile)
	firstFree := parseUInt16(databaseFile)
	numCells := parseUInt16(databaseFile)
	startOfCell := parseUInt16(databaseFile)
	fragmented := parseUInt8(databaseFile)

	return PageHearder{
		pageType:    pageType,
		firstFree:   firstFree,
		numCells:    numCells,
		startOfCell: startOfCell,
		fragmented:  fragmented,
	}

}
