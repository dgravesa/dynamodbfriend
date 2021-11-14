package dynamodbfriend

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Query returns a new QueryParser that may be used to retrieve query results.
func (table *Table) Query(ctx context.Context, expr *QueryExpr) (*QueryParser, error) {
	if expr.buildErr != nil {
		return nil, expr.buildErr
	}

	queryIndex, err := table.chooseIndex(ctx, expr)
	if err != nil {
		return nil, err
	}

	queryInput, err := expr.constructQueryInputGivenIndex(queryIndex)
	if err != nil {
		return nil, err
	}

	return &QueryParser{
		table:         table,
		expr:          expr,
		queryInput:    queryInput,
		bufferedItems: []map[string]*dynamodb.AttributeValue{},
	}, nil
}

func (table *Table) chooseIndex(ctx context.Context, expr *QueryExpr) (*tableIndex, error) {
	viableIndexNameSet, err := table.getViableQueryIndexes(ctx, expr)
	if err != nil {
		return nil, err
	}

	if viableIndexNameSet.Empty() {
		expr.logger.Printf("error: no viable indexes found in table \"%s\"\n", table.Name)
		return nil, ErrNoViableIndexes{TableName: table.Name, Expr: expr}
	}

	expr.logger.Printf("found viable indexes: %v\n", viableIndexNameSet.Names())

	priorityIndexNameSet := newNameSet()

	getPriorityIndexesWithSortKeyOnFilterType := func(v interface{}) bool {
		filterKeys := expr.getKeysOfFilterType(v)

		for _, indexName := range viableIndexNameSet.Names() {
			indexSortKey := table.allIndexes[indexName].SortKey
			if filterKeys.Contains(indexSortKey) {
				priorityIndexNameSet.Insert(indexName)
			}
		}

		return !priorityIndexNameSet.Empty()
	}

	// prioritization order for sort key:
	//	1) equals
	//	2) begins with
	//	3) between
	//	4) any viable index
	priorityIndexesFound := getPriorityIndexesWithSortKeyOnFilterType(&equalsFilter{}) ||
		getPriorityIndexesWithSortKeyOnFilterType(&beginsWithFilter{}) ||
		getPriorityIndexesWithSortKeyOnFilterType(&betweenFilter{})
	if !priorityIndexesFound {
		priorityIndexNameSet = viableIndexNameSet
	}

	// default to first priority index found
	// TODO: consider other prioritization metrics
	chosenIndexName := priorityIndexNameSet.Names()[0]
	expr.logger.Printf("choosing index for query: %s\n", chosenIndexName)

	return table.allIndexes[chosenIndexName], nil
}

func (table *Table) getViableQueryIndexes(ctx context.Context, expr *QueryExpr) (*nameSet, error) {
	// learn table indexes if not already known
	if table.allIndexes == nil {
		if err := table.fetchIndexMetadata(ctx); err != nil {
			return nil, err
		}
	}

	viableIndexNameSet := table.indexNameSet()
	expr.logger.Printf("found indexes in table \"%s\": %s\n",
		table.Name, viableIndexNameSet)

	filterIndexNames := func(failedDescription string, validCondition func(index *tableIndex) bool) {
		for _, indexName := range viableIndexNameSet.Names() {
			index := table.allIndexes[indexName]
			if !validCondition(index) {
				var indexKeysStr string
				if index.IsComposite {
					indexKeysStr = fmt.Sprintf("partition:\"%s\", sort:\"%s\"",
						index.PartitionKey, index.SortKey)
				} else {
					indexKeysStr = fmt.Sprintf("partition:\"%s\"", index.PartitionKey)
				}

				expr.logger.Printf("index \"%s\" [%s] not viable on condition: %s\n",
					indexName, indexKeysStr, failedDescription)

				viableIndexNameSet.Remove(indexName)
			}
		}
	}

	equalsFilterKeys := expr.getKeysOfFilterType(&equalsFilter{})
	failedDescription := fmt.Sprintf("partition key not in equals filters: %s", equalsFilterKeys)
	filterIndexNames(failedDescription, func(index *tableIndex) bool {
		return equalsFilterKeys.Contains(index.PartitionKey)
	})

	// omit indexes that do not support consistent read, if applicable
	if expr.consistentRead {
		filterIndexNames("index does not support consistent read", func(index *tableIndex) bool {
			return index.ConsistentReadable
		})
	}

	// omit indexes that do not sort on the requested order key, if applicable
	if expr.orderMatters {
		failedDescription := fmt.Sprintf(
			"index does not support sorting on \"%s\" attribute", expr.orderKey)
		filterIndexNames(failedDescription, func(index *tableIndex) bool {
			return index.IsComposite && index.SortKey == expr.orderKey
		})
	}

	// omit indexes that do not include all requested attributes
	if expr.attributesSpecified {
		failedDescription := "index does not include all selected attributes"
		filterIndexNames(failedDescription, func(index *tableIndex) bool {
			if index.IncludesAllAttributes {
				return true
			}
			for _, selectAttribute := range expr.attributes {
				if _, found := index.AttributeSet[selectAttribute]; !found {
					// missing queried attribute in index projection
					return false
				}
			}
			return true
		})
	} else {
		// if no projection is specified, query should return all attributes
		failedDescription := "index does not project all attributes"
		filterIndexNames(failedDescription, func(index *tableIndex) bool {
			return index.IncludesAllAttributes
		})
	}

	return viableIndexNameSet, nil
}
