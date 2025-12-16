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
			rootPage: toInt(record.values[3]),
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

func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int8:
		return int(n)
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint8:
		return int(n)
	case uint16:
		return int(n)
	case uint32:
		return int(n)
	case uint64:
		return int(n)
	default:
		log.Fatalf("unexpected integer type %T", v)
		return 0
	}
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

func countTableRows(databaseFile *os.File, rootPage int, pageSize int, whereExpr sqlparser.Expr, payloadCols []string, payloadIndex map[string]int, rowidColName string) int {
	count := 0
	countTableRowsRecursive(databaseFile, rootPage, pageSize, whereExpr, payloadCols, payloadIndex, rowidColName, &count)
	return count
}

func countTableRowsRecursive(databaseFile *os.File, pageNum int, pageSize int, whereExpr sqlparser.Expr, payloadCols []string, payloadIndex map[string]int, rowidColName string, count *int) {
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

			countTableRowsRecursive(databaseFile, int(leftChild), pageSize, whereExpr, payloadCols, payloadIndex, rowidColName, count)
		}

		countTableRowsRecursive(databaseFile, int(rightmostChild), pageSize, whereExpr, payloadCols, payloadIndex, rowidColName, count)

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

			if whereExpr == nil || evaluateWhereClause(whereExpr, payloadIndex, payloadCols, rowidColName, rec.values, rowid) {
				*count++
			}
		}
	}
}

func extractRowValues(rec Record, rowid int, colIndices []int, isRowidCols []bool) []string {
	values := make([]string, 0, len(colIndices))
	for i, colIndex := range colIndices {
		if isRowidCols[i] {
			values = append(values, fmt.Sprintf("%d", rowid))
			continue
		}
		if colIndex >= len(rec.values) {
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
	return values
}

func extractEqualityValue(expr sqlparser.Expr, column string) (string, bool) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator != sqlparser.EqualStr {
			return "", false
		}
		if col, ok := e.Left.(*sqlparser.ColName); ok && strings.EqualFold(col.Name.String(), column) {
			return literalToString(e.Right)
		}
		if col, ok := e.Right.(*sqlparser.ColName); ok && strings.EqualFold(col.Name.String(), column) {
			return literalToString(e.Left)
		}
	case *sqlparser.AndExpr:
		if v, ok := extractEqualityValue(e.Left, column); ok {
			return v, true
		}
		return extractEqualityValue(e.Right, column)
	}
	return "", false
}

func literalToString(expr sqlparser.Expr) (string, bool) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal, sqlparser.IntVal:
			return string(v.Val), true
		}
	}
	return "", false
}

func searchIndexForValue(databaseFile *os.File, indexRoot int, pageSize int, target string) []int {
	var rowids []int
	traverseIndexBTree(databaseFile, indexRoot, pageSize, target, &rowids)
	return rowids
}

func traverseIndexBTree(databaseFile *os.File, pageNum int, pageSize int, target string, rowids *[]int) {
	pageStart := int64((pageNum - 1) * pageSize)
	pageData := make([]byte, pageSize)
	n, err := databaseFile.ReadAt(pageData, pageStart)
	if err != nil && err != io.EOF {
		log.Fatalf("Error reading index page %d: %v", pageNum, err)
	}
	if n < 8 {
		log.Fatalf("Index page %d too small: only %d bytes", pageNum, n)
	}

	reader := bytes.NewReader(pageData)
	header := parserHeader(reader)

	switch header.pageType {
	case 0x02: // Interior index page
		rightmostChild := parseUInt32(reader)
		cellPointers := make([]uint16, header.numCells)
		for i := 0; i < int(header.numCells); i++ {
			cellPointers[i] = parseUInt16(reader)
		}

		for _, cellPtr := range cellPointers {
			reader.Seek(int64(cellPtr), io.SeekStart)
			leftChild := parseUInt32(reader)
			payloadSize := parseVarint(reader)
			rec := parserRecordDynamic(io.LimitReader(reader, int64(payloadSize)))
			if len(rec.values) == 0 {
				continue
			}
			keyVal := valueToString(rec.values[0])
			_ = keyVal
			traverseIndexBTree(databaseFile, int(leftChild), pageSize, target, rowids)
		}
		traverseIndexBTree(databaseFile, int(rightmostChild), pageSize, target, rowids)
	case 0x0A: // Leaf index page
		cellPointers := make([]uint16, header.numCells)
		for i := 0; i < int(header.numCells); i++ {
			cellPointers[i] = parseUInt16(reader)
		}
		for _, cellPtr := range cellPointers {
			reader.Seek(int64(cellPtr), io.SeekStart)
			payloadSize := parseVarint(reader) // payload size
			rec := parserRecordDynamic(io.LimitReader(reader, int64(payloadSize)))
			if len(rec.values) == 0 {
				continue
			}
			keyVal := valueToString(rec.values[0])
			if keyVal == target {
				rowidVal := rec.values[len(rec.values)-1]
				*rowids = append(*rowids, toInt(rowidVal))
			} else if strings.Compare(keyVal, target) > 0 {
				break
			}
		}
	default:
		log.Fatalf("Unsupported index page type: 0x%02X", header.pageType)
	}
}

func fetchTableRowByRowid(databaseFile *os.File, pageNum int, pageSize int, targetRowid int) (Record, bool) {
	pageStart := int64((pageNum - 1) * pageSize)
	pageData := make([]byte, pageSize)
	n, err := databaseFile.ReadAt(pageData, pageStart)
	if err != nil && err != io.EOF {
		log.Fatalf("Error reading table page %d: %v", pageNum, err)
	}
	if n < 8 {
		return Record{}, false
	}

	reader := bytes.NewReader(pageData)
	header := parserHeader(reader)

	switch header.pageType {
	case 0x05: // Interior table page
		rightmostChild := parseUInt32(reader)
		cellPointers := make([]uint16, header.numCells)
		for i := 0; i < int(header.numCells); i++ {
			cellPointers[i] = parseUInt16(reader)
		}

		for _, cellPtr := range cellPointers {
			reader.Seek(int64(cellPtr), io.SeekStart)
			leftChild := parseUInt32(reader)
			key := parseVarint(reader)
			if targetRowid <= key {
				return fetchTableRowByRowid(databaseFile, int(leftChild), pageSize, targetRowid)
			}
		}
		return fetchTableRowByRowid(databaseFile, int(rightmostChild), pageSize, targetRowid)

	case 0x0D: // Leaf table page
		cellPointers := make([]uint16, header.numCells)
		for i := 0; i < int(header.numCells); i++ {
			cellPointers[i] = parseUInt16(reader)
		}
		for _, cellPtr := range cellPointers {
			reader.Seek(int64(cellPtr), io.SeekStart)
			_ = parseVarint(reader)      // payload size
			rowid := parseVarint(reader) // rowid
			if rowid == targetRowid {
				rec := parserRecordDynamic(reader)
				return rec, true
			}
			if rowid > targetRowid {
				break
			}
		}
		return Record{}, false
	default:
		return Record{}, false
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
				if aliasedExpr, ok := stmt.SelectExprs[0].(*sqlparser.AliasedExpr); ok && strings.ToUpper(sqlparser.String(aliasedExpr.Expr)) == "COUNT(*)" {
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
					var (
						payloadCols  []string
						rowidColName string
						payloadIndex map[string]int
					)
					payloadIndex = make(map[string]int)
					recordIndex := 0
					for _, def := range defs {
						if def.isRowid {
							rowidColName = def.name
							recordIndex++
						} else {
							payloadCols = append(payloadCols, def.name)
							payloadIndex[strings.ToLower(def.name)] = recordIndex
							recordIndex++
						}
					}

					// Count rows using B-tree traversal
					var whereExpr sqlparser.Expr
					if stmt.Where != nil {
						whereExpr = stmt.Where.Expr
					}
					count := countTableRows(databaseFile, rootPage, pageSize, whereExpr, payloadCols, payloadIndex, rowidColName)
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
			payloadIndex := make(map[string]int)
			recordIndex := 0
			for _, def := range defs {
				if def.isRowid {
					rowidColName = def.name // Remember the name of the rowid column
					recordIndex++
				} else {
					payloadCols = append(payloadCols, def.name)
					payloadIndex[strings.ToLower(def.name)] = recordIndex
					recordIndex++
				}
			}

			// Optional: find index on country and country value from WHERE clause
			var indexRoot int
			for _, row := range sqliteSchemaRows {
				if row._type == "index" && row.tblName == tableName && row.name == "idx_companies_country" {
					indexRoot = row.rootPage
					break
				}
			}
			var countryValue string
			var hasCountryFilter bool
			if stmt.Where != nil {
				countryValue, hasCountryFilter = extractEqualityValue(stmt.Where.Expr, "country")
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
					colIndex, ok := payloadIndex[strings.ToLower(colName)]
					if !ok {
						fmt.Printf("no such column: %s\n", colName)
						fmt.Printf("Available payload columns: %v\n", payloadCols)
						fmt.Printf("Rowid column: %s\n", rowidColName)
						os.Exit(1)
					}
					colIndices = append(colIndices, colIndex)
					isRowidCols = append(isRowidCols, false)
				}
			}

			if indexRoot != 0 && hasCountryFilter {
				rowids := searchIndexForValue(databaseFile, indexRoot, pageSize, countryValue)
				seen := make(map[int]bool)
				for _, rowid := range rowids {
					if seen[rowid] {
						continue
					}
					seen[rowid] = true

					rec, ok := fetchTableRowByRowid(databaseFile, rootPage, pageSize, rowid)
					if !ok {
						continue
					}
					values := extractRowValues(rec, rowid, colIndices, isRowidCols)
					fmt.Println(strings.Join(values, "|"))
				}
				return
			}

			// Collect all rows using B-tree traversal
			allRows := collectAllTableRows(databaseFile, rootPage, pageSize)

			// Filter and print rows
			for _, row := range allRows {
				// Check WHERE clause if present
				if stmt.Where != nil {
					match := evaluateWhereClause(stmt.Where.Expr, payloadIndex, payloadCols, rowidColName, row.record.values, row.rowid)
					if !match {
						continue // Skip this row
					}
				}

				values := extractRowValues(row.record, row.rowid, colIndices, isRowidCols)
				fmt.Println(strings.Join(values, "|"))
			}

			return

		default:
			fmt.Println("Unsupported SQL statement type")
			os.Exit(1)
		}
	}
}
