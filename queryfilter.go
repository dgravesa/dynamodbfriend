package dynamodbfriend

type queryFilter interface {
	Key() string
}

type equalsFilter struct {
	key   string
	value interface{}
}

func (f equalsFilter) Key() string {
	return f.key
}

type lessThanFilter struct {
	key   string
	value interface{}
}

func (f lessThanFilter) Key() string {
	return f.key
}

type greaterThanFilter struct {
	key   string
	value interface{}
}

func (f greaterThanFilter) Key() string {
	return f.key
}

type lessThanEqualFilter struct {
	key   string
	value interface{}
}

func (f lessThanEqualFilter) Key() string {
	return f.key
}

type greaterThanEqualFilter struct {
	key   string
	value interface{}
}

func (f greaterThanEqualFilter) Key() string {
	return f.key
}

type beginsWithFilter struct {
	key    string
	prefix string
}

func (f beginsWithFilter) Key() string {
	return f.key
}

type betweenFilter struct {
	key             string
	lowval, highval interface{}
}

func (f betweenFilter) Key() string {
	return f.key
}
