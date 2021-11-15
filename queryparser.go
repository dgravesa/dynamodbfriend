package dynamodbfriend

import (
	"context"

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
	currentBufferIndex     int

	totalPagesParsed int
}

// Next retrieves the next value returned by the query. The val must be a non-nil pointer.
// The underlying query will only execute when new items are requested and any buffered items have
// already been consumed.
func (parser *QueryParser) Next(ctx context.Context, val interface{}) error {
	parsingComplete := func(reason string) error {
		err := ErrParsingComplete{reason: reason}
		parser.expr.logger.Printf("%s\n", err)
		return err
	}

	// execute a new query to refill the buffer if necessary
	// retry until new items are found or a parsing complete condition has been met
	for parser.currentBufferIndex == len(parser.bufferedItems) {
		if parser.allItemsParsed() {
			return parsingComplete("all items have been parsed")
		} else if parser.maxPaginationReached() {
			return parsingComplete("max pagination has been reached")
		}

		parser.queryInput.ExclusiveStartKey = parser.lastEvaluatedKey

		queryOutput, err := parser.table.baseClient.QueryWithContext(ctx, parser.queryInput)
		if err != nil {
			return err
		}

		parser.lastEvaluatedKey = queryOutput.LastEvaluatedKey
		parser.totalPagesParsed++
		parser.bufferedItems = queryOutput.Items
		parser.currentBufferIndex = 0
	}

	thisItem := parser.bufferedItems[parser.currentBufferIndex]
	parser.currentBufferIndex++

	return dynamodbattribute.UnmarshalMap(thisItem, val)
}

func (parser *QueryParser) lastEvaluatedKeyIsEmpty() bool {
	return parser.lastEvaluatedKey == nil || len(parser.lastEvaluatedKey) == 0
}

func (parser *QueryParser) allItemsParsed() bool {
	return parser.totalPagesParsed > 0 && parser.lastEvaluatedKeyIsEmpty()
}

func (parser *QueryParser) maxPaginationReached() bool {
	return parser.expr.maxPaginationSpecified &&
		parser.totalPagesParsed == parser.expr.maxPagination
}
