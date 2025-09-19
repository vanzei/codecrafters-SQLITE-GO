package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// ...existing code...

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

	switch expr.Operator {
	case sqlparser.EqualStr:
		return compareValues(leftVal, rightVal) == 0
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
		if strings.ToLower(colName) == "rowid" {
			return rowid
		}
		for i, name := range columnNames {
			if strings.EqualFold(name, colName) {
				if i < len(values) {
					return values[i]
				}
				return nil
			}
		}
		log.Fatalf("Column not found: %s", colName)
		return nil
	case *sqlparser.SQLVal:
		switch e.Type {
		case sqlparser.StrVal:
			return string(e.Val)
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
	return strings.Compare(leftStr, rightStr)
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

// ...existing code...
