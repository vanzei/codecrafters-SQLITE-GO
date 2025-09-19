package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/xwb1989/sqlparser"
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

	//Check if metacommand ( starts with . )
	if strings.HasPrefix(command, ".") {
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
			fmt.Println("Unknown command", command)
			os.Exit(1)
		}
	} else {
		// Otherwise, SQL command
		stmt, err := sqlparser.Parse(command)
		if err != nil {
			fmt.Println("Failed to parse SQL:", err)
			os.Exit(1)
		}

		switch stmt := stmt.(type) {
		case *sqlparser.Select:
			// Handle SELECT statements
			if len(stmt.From) != 1 {
				fmt.Println("Only single table SELECT statements are supported")
				os.Exit(1)
			}
			tableExpr := stmt.From[0]
			tableName := sqlparser.String(tableExpr.(*sqlparser.AliasedTableExpr).Expr)

			// Handle COUNT(*) special case
			if len(stmt.SelectExprs) == 1 {
				colExpr := stmt.SelectExprs[0]
				colName := sqlparser.String(colExpr.(*sqlparser.AliasedExpr).Expr)
				if strings.ToUpper(colName) == "COUNT(*)" {
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

					// Read table leaf page and count rows
					const pageTypeTableLeaf = 0x0D
					pageStart := int64((rootPage - 1) * pageSize)
					_, _ = databaseFile.Seek(pageStart, io.SeekStart)
					pageHeader := parserHeader(databaseFile)
					if pageHeader.pageType != pageTypeTableLeaf {
						log.Fatalf("only leaf table pages supported for SELECT, got pageType=0x%02X", pageHeader.pageType)
					}

					// If there's a WHERE clause, we need to count matching rows
					if stmt.Where != nil {
						count := 0
						// Read cell pointer array
						cellPointers := make([]uint16, pageHeader.numCells)
						for i := 0; i < int(pageHeader.numCells); i++ {
							cellPointers[i] = parseUInt16(databaseFile)
						}

						// Get table schema for WHERE evaluation
						var createSQL string
						for _, row := range sqliteSchemaRows {
							if row._type == "table" && row.name == tableName {
								createSQL = row.sql
								break
							}
						}
						if createSQL == "" {
							fmt.Printf("table schema not found: %s\n", tableName)
							os.Exit(1)
						}
						defs := parseCreateTableColumns(createSQL)
						var payloadCols []string
						for _, def := range defs {
							payloadCols = append(payloadCols, def.name)
						}

						// Count matching rows
						for _, cellPtr := range cellPointers {
							_, _ = databaseFile.Seek(pageStart+int64(cellPtr), io.SeekStart)
							_ = parseVarint(databaseFile)      // payload size
							rowid := parseVarint(databaseFile) // rowid
							rec := parserRecordDynamic(databaseFile)

							if evaluateWhereClause(stmt.Where.Expr, payloadCols, rec.values, rowid) {
								count++
							}
						}
						fmt.Println(count)
					} else {
						fmt.Println(pageHeader.numCells)
					}
					return
				}
			}

			// Parse requested columns
			var requestedCols []string
			for _, selectExpr := range stmt.SelectExprs {
				switch expr := selectExpr.(type) {
				case *sqlparser.AliasedExpr:
					colName := sqlparser.String(expr.Expr)
					requestedCols = append(requestedCols, colName)
				case *sqlparser.StarExpr:
					// Handle SELECT * - add all columns
					requestedCols = []string{"*"}
				default:
					fmt.Println("Unsupported SELECT expression type")
					os.Exit(1)
				}
			}

			var (
				rootPage  int
				createSQL string
			)

			for _, row := range sqliteSchemaRows {
				if row._type == "table" && row.name == tableName {
					rootPage = row.rootPage
					createSQL = row.sql
					break
				}
			}
			if rootPage == 0 {
				fmt.Printf("table not found: %s\n", tableName)
				os.Exit(1)
			}

			defs := parseCreateTableColumns(createSQL)

			// Build the list of all columns (both regular and rowid)
			var payloadCols []string
			for _, def := range defs {
				payloadCols = append(payloadCols, def.name)
			}

			// Handle SELECT *
			if len(requestedCols) == 1 && requestedCols[0] == "*" {
				requestedCols = make([]string, len(payloadCols))
				copy(requestedCols, payloadCols)
			}

			// Find column indices
			var colIndices []int
			var isRowidCols []bool
			for _, colName := range requestedCols {
				if strings.ToLower(colName) == "rowid" {
					colIndices = append(colIndices, -1) // Special marker for rowid
					isRowidCols = append(isRowidCols, true)
				} else {
					colIndex := -1
					for i, c := range payloadCols {
						if strings.EqualFold(c, colName) {
							colIndex = i
							break
						}
					}
					if colIndex == -1 {
						fmt.Printf("no such column: %s\n", colName)
						os.Exit(1)
					}
					colIndices = append(colIndices, colIndex)
					isRowidCols = append(isRowidCols, false)
				}
			}

			// Read table leaf page
			const pageTypeTableLeaf = 0x0D
			pageStart := int64((rootPage - 1) * pageSize)
			_, _ = databaseFile.Seek(pageStart, io.SeekStart)
			pageHeader := parserHeader(databaseFile)
			if pageHeader.pageType != pageTypeTableLeaf {
				log.Fatalf("only leaf table pages supported for SELECT, got pageType=0x%02X", pageHeader.pageType)
			}

			// Read cell pointer array
			cellPointers := make([]uint16, pageHeader.numCells)
			for i := 0; i < int(pageHeader.numCells); i++ {
				cellPointers[i] = parseUInt16(databaseFile)
			}

			// For each row (cell), parse and print the requested columns
			for _, cellPtr := range cellPointers {
				_, _ = databaseFile.Seek(pageStart+int64(cellPtr), io.SeekStart)
				_ = parseVarint(databaseFile)      // payload size (not used directly)
				rowid := parseVarint(databaseFile) // rowid

				rec := parserRecordDynamic(databaseFile)

				// Check WHERE clause if present
				if stmt.Where != nil {
					if !evaluateWhereClause(stmt.Where.Expr, payloadCols, rec.values, rowid) {
						continue // Skip this row
					}
				}

				// Print values for all requested columns
				var values []string
				for i, colIndex := range colIndices {
					if isRowidCols[i] {
						// This is the rowid column
						values = append(values, fmt.Sprintf("%d", rowid))
					} else {
						if colIndex >= len(rec.values) {
							// Safety check if schema parsing didn't match payload
							values = append(values, "")
							continue
						}
						v := rec.values[colIndex]
						switch vv := v.(type) {
						case nil:
							values = append(values, "")
						case []byte:
							values = append(values, string(vv))
						default:
							values = append(values, fmt.Sprintf("%v", vv))
						}
					}
				}

				// Print row with pipe-separated values (or tab-separated)
				fmt.Println(strings.Join(values, "|"))
			}
			return

		default:
			fmt.Println("Unsupported SQL statement type")
			os.Exit(1)
		}
	}
}
