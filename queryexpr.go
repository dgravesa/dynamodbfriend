package dynamodbfriend

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// QueryExpr is a fully-formed query expression.
type QueryExpr struct {
	filters map[string]queryFilter

	limitSpecified bool
	limit          int

	attributesSpecified bool
	attributes          []string

	orderMatters    bool
	orderKey        string
	orderDescending bool

	maxPaginationSpecified bool
	maxPagination          int

	consistentRead bool

	additionalConditions []expression.ConditionBuilder

	logger Logger

	buildErr error
}

// And extends a query with an additional query condition.
func (expr *QueryExpr) And(key string) *QueryExprKey {
	return &QueryExprKey{
		expr: expr,
		key:  key,
	}
}

// Limit restricts the number of items returnable by a query.
func (expr *QueryExpr) Limit(count int) *QueryExpr {
	expr.limitSpecified = true
	expr.limit = count
	expr.logger.Printf("query limit set to %d items\n", count)
	return expr
}

// Select restricts the attributes returned by a query.
func (expr *QueryExpr) Select(attributes ...string) *QueryExpr {
	expr.attributesSpecified = true
	expr.attributes = attributes
	expr.logger.Printf(
		"query requires index with projected attributes \"%v\" due to select statement\n",
		attributes)
	return expr
}

// OrderAscending sets the order of items returned based on values associated with a sort key,
// starting with the lowest value.
func (expr *QueryExpr) OrderAscending(sortKey string) *QueryExpr {
	expr.orderMatters = true
	expr.orderKey = sortKey
	expr.orderDescending = false
	expr.logger.Printf(
		"query requires index with \"%s\" as sort key due to order ascending\n", sortKey)
	return expr
}

// OrderDescending sets the order of items returned based on values associated with a sort key,
// starting with the highest value.
func (expr *QueryExpr) OrderDescending(sortKey string) *QueryExpr {
	expr.orderMatters = true
	expr.orderKey = sortKey
	expr.orderDescending = true
	expr.logger.Printf(
		"query requires index with \"%s\" as sort key due to order descending\n", sortKey)
	return expr
}

// MaxPagination restricts the number of paginated requests to make to DynamoDB. If the max
// pagination is reached and all items have been read, the iterator will return Done.
func (expr *QueryExpr) MaxPagination(count int) *QueryExpr {
	expr.maxPaginationSpecified = true
	expr.maxPagination = count
	expr.logger.Printf("max pagination of query set to %d\n", count)
	return expr
}

// ConsistentRead sets the read consistency.
// NOTE: For read consistency to be set to true, the partition key must be used with an Equals
// condition expression. Additionally, the max pagination will be set to 1.
func (expr *QueryExpr) ConsistentRead(val bool) *QueryExpr {
	expr.consistentRead = val
	if val == true {
		expr.maxPaginationSpecified = true
		expr.maxPagination = 1
		expr.logger.Printf(
			"query requires either primary index or local secondary index for consistent read\n")
		expr.logger.Printf("max pagination set to 1 for consistent read query")
	}
	return expr
}

// WithFilter applies an additional condition in addition to other filters on the query
// expression. This allows for filter conditions that are not otherwise supported by the query
// expression, such as OR conditions.
func (expr *QueryExpr) WithFilter(condition expression.ConditionBuilder) *QueryExpr {
	expr.additionalConditions = append(expr.additionalConditions, condition)
	return expr
}

// WithLogger sets a logger used to print logs about querying operations performed using this
// expression.
func (expr *QueryExpr) WithLogger(logger Logger) *QueryExpr {
	expr.logger = logger
	return expr
}

func (expr *QueryExpr) addFilter(v queryFilter, conditionName string) {
	key := v.Key()
	_, alreadyExists := expr.filters[key]
	if alreadyExists {
		err := fmt.Errorf("key \"%s\" already used in \"%s\" condition", key, conditionName)
		expr.logger.Printf("error: %s\n", err.Error())
		expr.buildErr = err
	} else {
		expr.filters[key] = v
	}
}

func (expr *QueryExpr) getKeysOfFilterType(v interface{}) *nameSet {
	getFilterType := reflect.TypeOf(v)

	// create set of all keys with specific filters
	keys := newNameSet()
	for key, filter := range expr.filters {
		thisFilterType := reflect.TypeOf(filter)
		if getFilterType == thisFilterType {
			keys.Insert(key)
		}
	}

	return keys
}

func (expr *QueryExpr) copyFilters() map[string]queryFilter {
	filters := map[string]queryFilter{}
	for k, v := range expr.filters {
		filters[k] = v
	}
	return filters
}

func (expr QueryExpr) constructQueryInputGivenIndex(index *tableIndex) (*dynamodb.QueryInput, error) {
	filters := expr.copyFilters()

	// initialize partition equals part of key condition expression
	dbExprBuilder := expression.NewBuilder()
	kce := expression.Key(index.PartitionKey).
		Equal(expression.Value(filters[index.PartitionKey].(*equalsFilter).value))
	delete(filters, index.PartitionKey)

	// apply sort key condition to key condition expression if applicable
	if index.IsComposite {
		filter, hasSortKeyFilter := filters[index.SortKey]
		if hasSortKeyFilter {
			builder := expression.Key(index.SortKey)
			switch f := filter.(type) {
			case *equalsFilter:
				kce = kce.And(builder.Equal(expression.Value(f.value)))
			case *lessThanFilter:
				kce = kce.And(builder.LessThan(expression.Value(f.value)))
			case *greaterThanFilter:
				kce = kce.And(builder.GreaterThan(expression.Value(f.value)))
			case *lessThanEqualFilter:
				kce = kce.And(builder.LessThanEqual(expression.Value(f.value)))
			case *greaterThanEqualFilter:
				kce = kce.And(builder.GreaterThanEqual(expression.Value(f.value)))
			case *betweenFilter:
				kce = kce.And(builder.Between(
					expression.Value(f.lowval), expression.Value(f.highval)))
			case *beginsWithFilter:
				kce = kce.And(builder.BeginsWith(f.prefix))
			default:
				err := fmt.Errorf("unknown filter type: %T", f)
				expr.logger.Printf("error: %s\n", err.Error())
				return nil, err
			}
			delete(filters, index.SortKey)
		}
	}

	dbExprBuilder = dbExprBuilder.WithKeyCondition(kce)

	// apply remaining filters as filter conditions
	filterConditions := []expression.ConditionBuilder{}
	for key, filter := range filters {
		var fc expression.ConditionBuilder
		switch f := filter.(type) {
		case *equalsFilter:
			fc = expression.Name(key).Equal(expression.Value(f.value))
		case *lessThanFilter:
			fc = expression.Name(key).LessThan(expression.Value(f.value))
		case *greaterThanFilter:
			fc = expression.Name(key).GreaterThan(expression.Value(f.value))
		case *lessThanEqualFilter:
			fc = expression.Name(key).LessThanEqual(expression.Value(f.value))
		case *greaterThanEqualFilter:
			fc = expression.Name(key).GreaterThanEqual(expression.Value(f.value))
		case *betweenFilter:
			fc = expression.Name(key).Between(
				expression.Value(f.lowval), expression.Value(f.highval))
		case *beginsWithFilter:
			fc = expression.Name(key).BeginsWith(f.prefix)
		default:
			err := fmt.Errorf("unknown filter type: %T", f)
			expr.logger.Printf("error: %s\n", err.Error())
			return nil, err
		}
		filterConditions = append(filterConditions, fc)
	}

	// apply additional filter conditions, if specified
	filterConditions = append(filterConditions, expr.additionalConditions...)

	if len(filterConditions) == 1 {
		dbExprBuilder = dbExprBuilder.WithFilter(filterConditions[0])
	} else if len(filterConditions) > 1 {
		dbExprBuilder = dbExprBuilder.WithFilter(expression.And(
			filterConditions[0],
			filterConditions[1],
			filterConditions[2:]...))
	}

	// set projection if specified
	if expr.attributesSpecified {
		names := []expression.NameBuilder{}
		for _, attribute := range expr.attributes {
			names = append(names, expression.Name(attribute))
		}
		proj := expression.NamesList(names[0], names[1:]...)
		dbExprBuilder = dbExprBuilder.WithProjection(proj)
	}

	dbExpr, err := dbExprBuilder.Build()
	if err != nil {
		return nil, err
	}

	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(index.TableName),
		KeyConditionExpression:    dbExpr.KeyCondition(),
		FilterExpression:          dbExpr.Filter(),
		ExpressionAttributeNames:  dbExpr.Names(),
		ExpressionAttributeValues: dbExpr.Values(),
		ProjectionExpression:      dbExpr.Projection(),
	}

	if index.Name != tablePrimaryIndexName {
		queryInput.IndexName = aws.String(index.Name)
	}

	if expr.limitSpecified {
		queryInput.Limit = aws.Int64(int64(expr.limit))
	}

	if expr.consistentRead {
		queryInput.ConsistentRead = aws.Bool(true)
	}

	if expr.orderMatters {
		queryInput.ScanIndexForward = aws.Bool(!expr.orderDescending)
	}

	return queryInput, nil
}
