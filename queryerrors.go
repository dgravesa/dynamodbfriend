package dynamodbfriend

import "fmt"

// ErrNoViableIndexes is returned when no viable indexes are found to execute a query expression
// on a table.
type ErrNoViableIndexes struct {
	TableName string
	Expr      *QueryExpr
}

func (e ErrNoViableIndexes) Error() string {
	// TODO: return a human-readable format for the query string, or better reasoning
	return fmt.Sprintf("no viable indexes found for table \"%s\" for given query", e.TableName)
}
