package dynamodbfriend

import "github.com/aws/aws-sdk-go/service/dynamodb/expression"

// QueryExprKey is a partially-formed query expression.
//
// To make a fully-formed query expression, the key part must be followed by a conditional.
type QueryExprKey struct {
	expr *QueryExpr
	key  string
}

// NewQuery begins a new query expression.
func NewQuery(key string) *QueryExprKey {
	return &QueryExprKey{
		expr: &QueryExpr{
			filters:              map[string]queryFilter{},
			additionalConditions: []expression.ConditionBuilder{},
			logger:               nullLogger{},
		},
		key: key,
	}
}

// Equals is a conditional where the value associated with a query key must equal val.
func (k *QueryExprKey) Equals(val interface{}) *QueryExpr {
	k.expr.addFilter(&equalsFilter{
		key:   k.key,
		value: val,
	}, "equals")

	return k.expr
}

// LessThan is a conditional where the value associated with a query key must be less
// than val.
func (k *QueryExprKey) LessThan(val interface{}) *QueryExpr {
	k.expr.addFilter(&lessThanFilter{
		key:   k.key,
		value: val,
	}, "less than")

	return k.expr
}

// GreaterThan is a conditional expression where the value associated with a query key must be
// greater than val.
func (k *QueryExprKey) GreaterThan(val interface{}) *QueryExpr {
	k.expr.addFilter(&greaterThanFilter{
		key:   k.key,
		value: val,
	}, "greater than")

	return k.expr
}

// LessThanEqual is a conditional expression where the value associated with a query key must be
// less than or equal to val.
func (k *QueryExprKey) LessThanEqual(val interface{}) *QueryExpr {
	k.expr.addFilter(&lessThanEqualFilter{
		key:   k.key,
		value: val,
	}, "less than or equal")

	return k.expr
}

// GreaterThanEqual is a conditional expression where the value associated with a query key must
// be greater than or equal to val.
func (k *QueryExprKey) GreaterThanEqual(val interface{}) *QueryExpr {
	k.expr.addFilter(&greaterThanEqualFilter{
		key:   k.key,
		value: val,
	}, "greater than or equal")

	return k.expr
}

// Between is a conditional expression where the value associated with a query key must be between
// lowval and highval.
func (k *QueryExprKey) Between(lowval, highval interface{}) *QueryExpr {
	k.expr.addFilter(&betweenFilter{
		key:     k.key,
		lowval:  lowval,
		highval: highval,
	}, "between")

	return k.expr
}

// BeginsWith is a conditional expression where the value associated with a query key must begin
// with a specified prefix.
func (k *QueryExprKey) BeginsWith(prefix string) *QueryExpr {
	k.expr.addFilter(&beginsWithFilter{
		key:    k.key,
		prefix: prefix,
	}, "begins with")

	return k.expr
}
