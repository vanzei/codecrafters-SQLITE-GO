package main

import (
	"strings"
)

type columnDef struct {
	name    string
	isRowid bool
}

func parseCreateTableColumns(createSQL string) []columnDef {
	//fmt.Printf("Parsing CREATE SQL: %s\n", createSQL)

	start := strings.Index(createSQL, "(")
	end := strings.LastIndex(createSQL, ")")
	if start < 0 || end < 0 || end <= start {
		return nil
	}
	inside := createSQL[start+1 : end]
	parts := strings.Split(inside, ",")
	var cols []columnDef

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		//fmt.Printf("Processing column definition %d: '%s'\n", i, p)

		// First token is the column name (may be quoted)
		tokens := strings.Fields(p)
		if len(tokens) == 0 {
			continue
		}
		rawName := tokens[0]
		name := strings.Trim(rawName, "`\"[]") // strip common quotes
		up := strings.ToUpper(p)

		// Check if this is an integer primary key column
		isRowid := false

		// Pattern 1: INTEGER PRIMARY KEY
		if strings.Contains(up, "INTEGER") && strings.Contains(up, "PRIMARY") && strings.Contains(up, "KEY") {
			isRowid = true
			//fmt.Printf("  -> Detected as INTEGER PRIMARY KEY\n")
		}

		// Pattern 2: INT PRIMARY KEY (but not POINT, JOINT, etc.)
		if !isRowid && strings.Contains(up, "INT") && strings.Contains(up, "PRIMARY") && strings.Contains(up, "KEY") {
			// Make sure it's "INT" and not part of another word
			words := strings.Fields(up)
			for _, word := range words {
				if word == "INT" {
					isRowid = true
					//fmt.Printf("  -> Detected as INT PRIMARY KEY\n")
					break
				}
			}
		}

		// Pattern 3: Just PRIMARY KEY on what looks like an ID column
		if !isRowid && strings.Contains(up, "PRIMARY") && strings.Contains(up, "KEY") {
			lowerName := strings.ToLower(name)
			if lowerName == "id" || lowerName == "rowid" || strings.HasSuffix(lowerName, "_id") {
				isRowid = true
				//fmt.Printf("  -> Detected as PRIMARY KEY on ID-like column\n")
			}
		}

		cols = append(cols, columnDef{name: name, isRowid: isRowid})
		//fmt.Printf("  -> Column: %s, isRowid: %v\n", name, isRowid)
	}
	return cols
}
