package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

type SQLiteSchemaRow struct {
	_type    string // _type since type is a reserved keyword
	name     string
	tblName  string
	rootPage int
	sql      string
}

// Reads the schema table and returns all rows and the page size
func readSchemaTable(databaseFile *os.File) ([]SQLiteSchemaRow, int, error) {
	header := make([]byte, 100)
	_, err := databaseFile.ReadAt(header, 0)
	if err != nil {
		return nil, 0, err
	}
	pageSize := int(header[16])<<8 | int(header[17])
	_, _ = databaseFile.Seek(100, io.SeekStart)

	pageHeader := parserHeader(databaseFile)
	cellPointers := make([]uint16, pageHeader.numCells)
	for i := 0; i < int(pageHeader.numCells); i++ {
		cellPointers[i] = parseUInt16(databaseFile)
	}

	var sqliteSchemaRows []SQLiteSchemaRow
	for _, cellPointer := range cellPointers {
		_, _ = databaseFile.Seek(int64(cellPointer), io.SeekStart)
		parseVarint(databaseFile)
		parseVarint(databaseFile)
		record := parserRecord(databaseFile, 5)
		sqliteSchemaRows = append(sqliteSchemaRows, SQLiteSchemaRow{
			_type:    string(record.values[0].([]byte)),
			name:     string(record.values[1].([]byte)),
			tblName:  string(record.values[2].([]byte)),
			rootPage: int(record.values[3].(uint8)),
			sql:      string(record.values[4].([]byte)),
		})
	}
	return sqliteSchemaRows, pageSize, nil
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	sqliteSchemaRows, pageSize, err := readSchemaTable(databaseFile)
	if err != nil {
		log.Fatal(err)
	}

	switch command {
	case ".dbinfo":
		fmt.Println("database page size: ", pageSize)
		fmt.Printf("number of tables: %v\n", len(sqliteSchemaRows))
	case ".tables":
		var tableNames []string
		for _, row := range sqliteSchemaRows {
			tableNames = append(tableNames, row.tblName)
		}
		fmt.Println(strings.Join(tableNames, " "))
	default:
		if strings.HasPrefix(command, "SELECT COUNT(*) FROM ") || strings.HasPrefix(command, "select count(*) from ") {
			tableName := strings.TrimPrefix(command, "SELECT COUNT(*) FROM ")
			tableName = strings.TrimPrefix(tableName, "select count(*) from ")
			tableName = strings.TrimSpace(tableName)

			var rootPage int
			for _, row := range sqliteSchemaRows {
				if row._type == "table" && row.name == tableName {
					rootPage = row.rootPage
					break
				}
			}
			if rootPage == 0 {
				fmt.Printf("table not found: %s\n", tableName)
				os.Exit(1)
			}

			rootPageOffset := int64((rootPage - 1) * pageSize)
			_, _ = databaseFile.Seek(rootPageOffset, io.SeekStart)
			tablePageHeader := parserHeader(databaseFile)
			fmt.Println(tablePageHeader.numCells)
			return
		}
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
