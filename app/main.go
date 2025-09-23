package main

import (
	"bytes"
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

// Collects all rows from a table by traversing the B-tree
func collectAllTableRows(databaseFile *os.File, rootPageNum int, pageSize int) []TableRow {
	var allRows []TableRow

	// Start traversal from the root page
	traverseTableBTree(databaseFile, rootPageNum, pageSize, &allRows)

	return allRows
}

type TableRow struct {
	rowid  int
	record Record
}

// Recursively traverses the B-tree to collect all table rows
func traverseTableBTree(databaseFile *os.File, pageNum int, pageSize int, allRows *[]TableRow) {
	// Calculate page start position
	pageStart := int64((pageNum - 1) * pageSize)

	// Read the entire page into memory for safer access
	pageData := make([]byte, pageSize)
	n, err := databaseFile.ReadAt(pageData, pageStart)
	if err != nil && err != io.EOF {
		log.Fatalf("Error reading page %d: %v", pageNum, err)
	}
	if n < 12 { // Need at least 12 bytes for interior page
		log.Fatalf("Page %d too small: only %d bytes", pageNum, n)
	}

	// Create a reader for this page
	pageReader := bytes.NewReader(pageData)

	// Read the page header (8 bytes)
	pageHeader := parserHeader(pageReader)

	// fmt.Printf("Page %d: type=0x%02X, cells=%d\n", pageNum, pageHeader.pageType, pageHeader.numCells)

	switch pageHeader.pageType {
	case 0x05: // Interior table b-tree page
		// Interior page structure:
		// Bytes 0-7:   Page header
		// Bytes 8-11:  Rightmost child page number (4 bytes)
		// Bytes 12+:   Cell pointer array (2 bytes per cell)

		// Read the rightmost child page number (immediately after 8-byte header)
		rightmostChild := parseUInt32(pageReader)
		// fmt.Printf("Interior page %d: rightmost child = %d\n", pageNum, rightmostChild)

		// Validate rightmost child page number
		if rightmostChild == 0 || rightmostChild > 1000000 {
			log.Fatalf("Invalid rightmost child page number: %d", rightmostChild)
		}

		// Read cell pointer array
		cellPointers := make([]uint16, pageHeader.numCells)
		for i := 0; i < int(pageHeader.numCells); i++ {
			cellPointers[i] = parseUInt16(pageReader)
		}

		// Process each cell in the interior page
		for i, cellPtr := range cellPointers {
			if int(cellPtr) >= len(pageData) || cellPtr < 12 {
				log.Fatalf("Invalid cell pointer %d in page %d (page size %d)", cellPtr, pageNum, len(pageData))
			}

			// Seek to the cell
			pageReader.Seek(int64(cellPtr), io.SeekStart)

			// Each interior cell contains:
			// - 4 bytes: left child page number
			// - varint: key (rowid for table b-trees)
			leftChild := parseUInt32(pageReader)
			_ = parseVarint(pageReader) // key (rowid)

			// fmt.Printf("Interior cell %d: left child = %d, key = %d\n", i, leftChild, key)

			// Validate left child page number
			if leftChild == 0 || leftChild > 1000000 {
				log.Fatalf("Invalid left child page number: %d in cell %d", leftChild, i)
			}

			// Recursively traverse the left child
			traverseTableBTree(databaseFile, int(leftChild), pageSize, allRows)
		}

		// Finally, traverse the rightmost child
		traverseTableBTree(databaseFile, int(rightmostChild), pageSize, allRows)

	case 0x0D: // Leaf table b-tree page
		// Leaf page structure:
		// Bytes 0-7:   Page header
		// Bytes 8+:    Cell pointer array (2 bytes per cell)

		// Read cell pointer array (comes right after the 8-byte header)
		cellPointers := make([]uint16, pageHeader.numCells)
		for i := 0; i < int(pageHeader.numCells); i++ {
			cellPointers[i] = parseUInt16(pageReader)
		}

		// fmt.Printf("Leaf page %d: %d cells\n", pageNum, len(cellPointers))

		// Process each cell in the leaf page
		for _, cellPtr := range cellPointers {
			if int(cellPtr) >= len(pageData) || cellPtr < 8 {
				log.Fatalf("Invalid cell pointer %d in page %d (page size %d)", cellPtr, pageNum, len(pageData))
			}

			pageReader.Seek(int64(cellPtr), io.SeekStart)

			// Each leaf cell contains:
			// - varint: payload size (total size of the payload)
			// - varint: rowid
			// - payload: the actual record data
			payloadSize := parseVarint(pageReader)
			rowid := parseVarint(pageReader)

			// fmt.Printf("Leaf cell %d: payload size = %d, rowid = %d\n", i, payloadSize, rowid)

			// Check if we have enough data for the payload
			currentPos, _ := pageReader.Seek(0, io.SeekCurrent)
			remainingBytes := int64(len(pageData)) - currentPos
			if remainingBytes < int64(payloadSize) {
				log.Fatalf("Not enough data for payload: need %d bytes, have %d", payloadSize, remainingBytes)
			}

			rec := parserRecordDynamic(pageReader)

			// Add this row to our collection
			*allRows = append(*allRows, TableRow{
				rowid:  rowid,
				record: rec,
			})
		}

	default:
		log.Fatalf("Unsupported page type for table traversal: 0x%02X", pageHeader.pageType)
	}
}

func countTableRows(databaseFile *os.File, rootPage int, pageSize int, whereExpr sqlparser.Expr, payloadCols []string) int {
	count := 0
	countTableRowsRecursive(databaseFile, rootPage, pageSize, whereExpr, payloadCols, &count)
	return count
}

func countTableRowsRecursive(databaseFile *os.File, pageNum int, pageSize int, whereExpr sqlparser.Expr, payloadCols []string, count *int) {
	pageStart := int64((pageNum - 1) * pageSize)

	// Read the entire page into memory for safer access
	pageData := make([]byte, pageSize)
	n, err := databaseFile.ReadAt(pageData, pageStart)
	if err != nil && err != io.EOF {
		log.Fatalf("Error reading page %d: %v", pageNum, err)
	}
	if n < 8 {
		log.Fatalf("Page %d too small: only %d bytes", pageNum, n)
	}

	// Create a reader for this page
	pageReader := bytes.NewReader(pageData)
	pageHeader := parserHeader(pageReader)

	switch pageHeader.pageType {
	case 0x05: // Interior table b-tree page
		// Read rightmost child first (immediately after 8-byte header)
		rightmostChild := parseUInt32(pageReader)

		// Read cell pointer array
		cellPointers := make([]uint16, pageHeader.numCells)
		for i := 0; i < int(pageHeader.numCells); i++ {
			cellPointers[i] = parseUInt16(pageReader)
		}

		// Process each cell
		for _, cellPtr := range cellPointers {
			pageReader.Seek(int64(cellPtr), io.SeekStart)
			leftChild := parseUInt32(pageReader)
			_ = parseVarint(pageReader) // key

			countTableRowsRecursive(databaseFile, int(leftChild), pageSize, whereExpr, payloadCols, count)
		}

		countTableRowsRecursive(databaseFile, int(rightmostChild), pageSize, whereExpr, payloadCols, count)

	case 0x0D: // Leaf table b-tree page
		// Read cell pointer array
		cellPointers := make([]uint16, pageHeader.numCells)
		for i := 0; i < int(pageHeader.numCells); i++ {
			cellPointers[i] = parseUInt16(pageReader)
		}

		for _, cellPtr := range cellPointers {
			pageReader.Seek(int64(cellPtr), io.SeekStart)
			_ = parseVarint(pageReader)      // payload size
			rowid := parseVarint(pageReader) // rowid
			rec := parserRecordDynamic(pageReader)

			if whereExpr == nil || evaluateWhereClause(whereExpr, payloadCols, rec.values, rowid) {
				*count++
			}
		}
	}
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

					// Get table schema for WHERE evaluation
					var createSQL string
					for _, row := range sqliteSchemaRows {
						if row._type == "table" && row.name == tableName {
							createSQL = row.sql
							break
						}
					}
					defs := parseCreateTableColumns(createSQL)
					var payloadCols []string
					for _, def := range defs {
						if !def.isRowid { // Only add non-rowid columns to payloadCols
							payloadCols = append(payloadCols, def.name)
						}
					}

					// Count rows using B-tree traversal
					var whereExpr sqlparser.Expr
					if stmt.Where != nil {
						whereExpr = stmt.Where.Expr
					}
					count := countTableRows(databaseFile, rootPage, pageSize, whereExpr, payloadCols)
					fmt.Println(count)
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

			// Build the list of payload columns (exclude rowid columns) and track rowid column name
			var payloadCols []string
			var rowidColName string
			for _, def := range defs {
				if def.isRowid {
					rowidColName = def.name // Remember the name of the rowid column
				} else {
					payloadCols = append(payloadCols, def.name)
				}
			}

			// Handle SELECT *
			if len(requestedCols) == 1 && requestedCols[0] == "*" {
				requestedCols = make([]string, 0)
				// Add rowid column first if it exists
				if rowidColName != "" {
					requestedCols = append(requestedCols, rowidColName)
				}
				// Add all payload columns
				requestedCols = append(requestedCols, payloadCols...)
			}

			// Find column indices
			var colIndices []int
			var isRowidCols []bool
			for _, colName := range requestedCols {
				// Check if this is a rowid column (either explicit "rowid" or the detected rowid column name)
				if strings.ToLower(colName) == "rowid" || (rowidColName != "" && strings.EqualFold(colName, rowidColName)) {
					colIndices = append(colIndices, -1) // Special marker for rowid
					isRowidCols = append(isRowidCols, true)
				} else {
					colIndex := -1
					for i, c := range payloadCols {
						if strings.EqualFold(c, colName) {
							// Add 1 to account for the extra null column at the beginning
							colIndex = i + 1
							break
						}
					}
					if colIndex == -1 {
						fmt.Printf("no such column: %s\n", colName)
						fmt.Printf("Available payload columns: %v\n", payloadCols)
						fmt.Printf("Rowid column: %s\n", rowidColName)
						os.Exit(1)
					}
					colIndices = append(colIndices, colIndex)
					isRowidCols = append(isRowidCols, false)
				}
			}

			// Collect all rows using B-tree traversal
			allRows := collectAllTableRows(databaseFile, rootPage, pageSize)

			// Filter and print rows
			for _, row := range allRows {
				// Check WHERE clause if present
				if stmt.Where != nil {
					match := evaluateWhereClause(stmt.Where.Expr, payloadCols, row.record.values, row.rowid)
					if !match {
						continue // Skip this row
					}
				}

				// Print values for all requested columns
				var values []string
				for i, colIndex := range colIndices {
					if isRowidCols[i] {
						// This is the rowid column
						values = append(values, fmt.Sprintf("%d", row.rowid))
					} else {
						if colIndex >= len(row.record.values) {
							// Safety check if schema parsing didn't match payload
							values = append(values, "")
							continue
						}
						v := row.record.values[colIndex]
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

				// Print row with pipe-separated values
				fmt.Println(strings.Join(values, "|"))
			}

			return

		default:
			fmt.Println("Unsupported SQL statement type")
			os.Exit(1)
		}
	}
}
