package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func evaluateWhereClause(expr sqlparser.Expr, columnNames []string, values []interface{}, rowid int) bool {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return evaluateComparison(e, columnNames, values, rowid)
	case *sqlparser.AndExpr:
		return evaluateWhereClause(e.Left, columnNames, values, rowid) &&
			evaluateWhereClause(e.Right, columnNames, values, rowid)
	case *sqlparser.OrExpr:
		return evaluateWhereClause(e.Left, columnNames, values, rowid) ||
			evaluateWhereClause(e.Right, columnNames, values, rowid)
	case *sqlparser.ParenExpr:
		return evaluateWhereClause(e.Expr, columnNames, values, rowid)
	default:
		log.Fatalf("Unsupported WHERE expression type: %T", expr)
		return false
	}
}

func evaluateComparison(expr *sqlparser.ComparisonExpr, columnNames []string, values []interface{}, rowid int) bool {
	// Get left operand value
	leftVal := getExprValue(expr.Left, columnNames, values, rowid)
	rightVal := getExprValue(expr.Right, columnNames, values, rowid)

	//fmt.Printf("    Comparing: %v (%T) %s %v (%T)\n", leftVal, leftVal, expr.Operator, rightVal, rightVal)

	switch expr.Operator {
	case sqlparser.EqualStr:
		result := compareValues(leftVal, rightVal) == 0
		//fmt.Printf("    Comparison result: %v\n", result)
		return result
	case sqlparser.NotEqualStr:
		return compareValues(leftVal, rightVal) != 0
	case sqlparser.LessThanStr:
		return compareValues(leftVal, rightVal) < 0
	case sqlparser.LessEqualStr:
		return compareValues(leftVal, rightVal) <= 0
	case sqlparser.GreaterThanStr:
		return compareValues(leftVal, rightVal) > 0
	case sqlparser.GreaterEqualStr:
		return compareValues(leftVal, rightVal) >= 0
	default:
		log.Fatalf("Unsupported comparison operator: %s", expr.Operator)
		return false
	}
}

func getExprValue(expr sqlparser.Expr, columnNames []string, values []interface{}, rowid int) interface{} {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		colName := e.Name.String()
		//fmt.Printf("    Looking for column: %s\n", colName)
		// Handle rowid/id columns
		if strings.ToLower(colName) == "rowid" || strings.ToLower(colName) == "id" {
			fmt.Printf("    Found rowid column: %d\n", rowid)
			return rowid
		}
		// Look for column in the payload columns
		for i, name := range columnNames {
			if strings.EqualFold(name, colName) {
				// Add 1 to account for the extra null column at the beginning
				actualIndex := i + 1
				if actualIndex < len(values) {
					//fmt.Printf("    Found column %s at payload index %d (actual record index %d): %v (%T)\n", colName, i, actualIndex, values[actualIndex], values[actualIndex])
					return values[actualIndex]
				}
				return nil
			}
		}
		// If column not found in payload, it might be a rowid column
		log.Fatalf("Column not found: %s (available: %v)", colName, columnNames)
		return nil
	case *sqlparser.SQLVal:
		switch e.Type {
		case sqlparser.StrVal:
			result := string(e.Val)
			//fmt.Printf("    String literal: %s\n", result)
			return result
		case sqlparser.IntVal:
			// Parse integer
			val := string(e.Val)
			if num, err := strconv.Atoi(val); err == nil {
				return num
			}
			return val
		case sqlparser.FloatVal:
			// Parse float
			val := string(e.Val)
			if num, err := strconv.ParseFloat(val, 64); err == nil {
				return num
			}
			return val
		default:
			return string(e.Val)
		}
	default:
		log.Fatalf("Unsupported expression type in WHERE: %T", expr)
		return nil
	}
}

func compareValues(left, right interface{}) int {
	// Handle NULL values
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	// Convert both values to strings for comparison
	leftStr := valueToString(left)
	rightStr := valueToString(right)

	//fmt.Printf("    String comparison: '%s' vs '%s'\n", leftStr, rightStr)

	// Try numeric comparison first
	if leftNum, leftErr := strconv.ParseFloat(leftStr, 64); leftErr == nil {
		if rightNum, rightErr := strconv.ParseFloat(rightStr, 64); rightErr == nil {
			if leftNum < rightNum {
				return -1
			} else if leftNum > rightNum {
				return 1
			} else {
				return 0
			}
		}
	}

	// Fall back to string comparison
	result := strings.Compare(leftStr, rightStr)
	//fmt.Printf("    Final string comparison result: %d\n", result)
	return result
}

func valueToString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int:
		return strconv.Itoa(v)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}
