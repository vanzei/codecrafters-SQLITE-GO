package main

import (
	"io"
)

type PageHeader struct {
	pageType    uint8
	firstFree   uint16
	numCells    uint16
	startOfCell uint16
	fragmented  uint8
}

func parserHeader(stream io.Reader) PageHeader {

	pageType := parseUInt8(stream)
	firstFree := parseUInt16(stream)
	numCells := parseUInt16(stream)
	startOfCell := parseUInt16(stream)
	fragmented := parseUInt8(stream)

	return PageHeader{
		pageType:    pageType,
		firstFree:   firstFree,
		numCells:    numCells,
		startOfCell: startOfCell,
		fragmented:  fragmented,
	}

}
