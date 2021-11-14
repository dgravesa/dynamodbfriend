package dynamodbfriend

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// QueryParser is used for parsing query results.
// The query is executed lazily. The underlying query will only happen when new items are
// requested and all buffered items have already been consumed.
type QueryParser struct {
	table *Table

	expr             *QueryExpr
	queryInput       *dynamodb.QueryInput
	lastEvaluatedKey map[string]*dynamodb.AttributeValue

	bufferedItemsRemaining int
	bufferedItems          []map[string]*dynamodb.AttributeValue

	totalItemsParsed int
	totalPagesParsed int
	allPagesParsed   bool

	parsingComplete bool
}

// ParsingComplete is the error returned by QueryParser's Next when parsing has completed, either
// by all query items being consumed, the limit being reached, or the max pagination being
// reached.
var ParsingComplete = errors.New("parsing is complete")

// Next retrieves the next value returned by the query. The val must be a non-nil pointer.
// The underlying query will only execute when new items are requested and any buffered items have
// already been consumed.
func (parser *QueryParser) Next(ctx context.Context, val interface{}) error {
	if parser.parsingComplete {
		return ParsingComplete
	}

	// execute a new query to refill the buffer if necessary
	if parser.bufferedItemsRemaining == 0 {
		if parser.allPagesParsed {
			parser.expr.logger.Printf("all pages have been parsed\n")
			parser.parsingComplete = true
			return ParsingComplete
		} else if parser.expr.maxPaginationSpecified &&
			parser.totalPagesParsed == parser.expr.maxPagination {
			parser.expr.logger.Printf("max pagination has been reached\n")
			parser.parsingComplete = true
			return ParsingComplete
		}

		parser.queryInput.ExclusiveStartKey = parser.lastEvaluatedKey

		queryOutput, err := parser.table.baseClient.QueryWithContext(ctx, parser.queryInput)
		if err != nil {
			return err
		}

		// TODO: may be an issue of items being parsed but not matching on filter condition
		if len(queryOutput.Items) == 0 {
			parser.expr.logger.Printf("no items returned from query\n")
			parser.parsingComplete = true
			return ParsingComplete
		}

		if lastEvaluatedKeyIsEmpty(queryOutput.LastEvaluatedKey) {
			parser.allPagesParsed = true
		} else {
			parser.lastEvaluatedKey = queryOutput.LastEvaluatedKey
		}

		parser.totalPagesParsed++
		parser.bufferedItems = queryOutput.Items
		parser.bufferedItemsRemaining = len(queryOutput.Items)
	}

	thisItemIndex := len(parser.bufferedItems) - parser.bufferedItemsRemaining
	thisItem := parser.bufferedItems[thisItemIndex]
	parser.bufferedItemsRemaining--
	parser.totalItemsParsed++

	if parser.totalItemsParsed == parser.expr.limit {
		parser.expr.logger.Printf("parsing has reached limit of %d\n", parser.expr.limit)
		parser.parsingComplete = true
	}

	return dynamodbattribute.UnmarshalMap(thisItem, val)
}

func lastEvaluatedKeyIsEmpty(lastEvaluatedKey map[string]*dynamodb.AttributeValue) bool {
	return lastEvaluatedKey == nil || len(lastEvaluatedKey) == 0
}
