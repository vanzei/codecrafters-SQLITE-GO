package main

import (
	"strings"
)

type columnDef struct {
	name    string
	isRowid bool
}

func parseCreateTableColumns(createSQL string) []columnDef {
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
		// First token is the column name (may be quoted)
		tokens := strings.Fields(p)
		if len(tokens) == 0 {
			continue
		}
		rawName := tokens[0]
		name := strings.Trim(rawName, "`\"[]") // strip common quotes
		up := strings.ToUpper(p)
		isRowid := strings.Contains(up, "PRIMARY KEY") && strings.Contains(up, "INTEGER")
		cols = append(cols, columnDef{name: name, isRowid: isRowid})
		//fmt.Printf("Column: %s, isRowid: %v\n", name, isRowid)
	}
	return cols
}
