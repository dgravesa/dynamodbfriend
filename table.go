package dynamodbfriend

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Table represents a DynamoDB table.
// This type keeps the table name ready for all calls to the underlying DynamoDB client.
type Table struct {
	Name string

	baseClient dynamodbiface.DynamoDBAPI

	allIndexes map[string]*tableIndex
}

type tableIndex struct {
	Name                  string
	TableName             string
	PartitionKey          string
	SortKey               string
	IsComposite           bool
	AttributeSet          map[string]struct{}
	IncludesAllAttributes bool
	Size                  int
	ConsistentReadable    bool
}

// Table instantiates a new Table instance from a Client. This operation only sets metadata for
// subsequent requests and is guaranteed to succeed.
func (client *Client) Table(tableName string) *Table {
	return &Table{
		baseClient: client.Base,
		Name:       tableName,
	}
}

const tablePrimaryIndexName = "#primary"

func (table *Table) indexNameSet() *nameSet {
	indexNames := newNameSet()

	if table.allIndexes != nil {
		for indexName := range table.allIndexes {
			indexNames.Insert(indexName)
		}
	}

	return indexNames
}

func (table *Table) fetchIndexMetadata() error {
	table.allIndexes = nil

	// make call to AWS describe table
	describeInfo, err := table.baseClient.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(table.Name),
	})
	if err != nil {
		return err
	}

	tableDescription := describeInfo.Table

	table.allIndexes = map[string]*tableIndex{}

	// extract primary key index
	tablePrimaryIndex := new(tableIndex)
	tablePrimaryIndex.Name = tablePrimaryIndexName
	tablePrimaryIndex.TableName = table.Name
	tablePrimaryIndex.Size = int(*tableDescription.ItemCount)
	tablePrimaryIndex.loadKeysFromSchema(tableDescription.KeySchema)
	tablePrimaryIndex.IncludesAllAttributes = true
	tablePrimaryIndex.ConsistentReadable = true // true for table primary index
	table.allIndexes[tablePrimaryIndexName] = tablePrimaryIndex

	tablePrimaryIndexKeys := tablePrimaryIndex.getKeys()

	// extract global secondary indexes
	for _, indexDescription := range tableDescription.GlobalSecondaryIndexes {
		index := new(tableIndex)
		index.Name = *indexDescription.IndexName
		index.TableName = table.Name
		index.Size = int(*indexDescription.ItemCount)
		index.loadKeysFromSchema(indexDescription.KeySchema)
		index.loadAttributesFromProjection(indexDescription.Projection, tablePrimaryIndexKeys)
		index.ConsistentReadable = false // false for global secondary indexes
		table.allIndexes[index.Name] = index
	}

	// extract local secondary indexes
	for _, indexDescription := range tableDescription.LocalSecondaryIndexes {
		index := new(tableIndex)
		index.Name = *indexDescription.IndexName
		index.TableName = table.Name
		index.Size = int(*indexDescription.ItemCount)
		index.loadKeysFromSchema(indexDescription.KeySchema)
		index.loadAttributesFromProjection(indexDescription.Projection, tablePrimaryIndexKeys)
		index.ConsistentReadable = true // true for local secondary indexes
		table.allIndexes[index.Name] = index
	}

	return nil
}

func (index *tableIndex) loadKeysFromSchema(keySchema []*dynamodb.KeySchemaElement) {
	index.IsComposite = false
	for _, keyElement := range keySchema {
		switch *keyElement.KeyType {
		case "HASH":
			index.PartitionKey = *keyElement.AttributeName
		case "RANGE":
			index.SortKey = *keyElement.AttributeName
			index.IsComposite = true
		}
	}
}

func (index tableIndex) getKeys() []string {
	if index.IsComposite {
		return []string{index.PartitionKey, index.SortKey}
	}
	return []string{index.PartitionKey}
}

func (index *tableIndex) loadAttributesFromProjection(projection *dynamodb.Projection, tablePrimaryIndexKeys []string) {
	if projection == nil || *projection.ProjectionType == "ALL" {
		index.IncludesAllAttributes = true
	} else {
		index.IncludesAllAttributes = false
		index.AttributeSet = map[string]struct{}{}
		// include keys
		for _, key := range append(index.getKeys(), tablePrimaryIndexKeys...) {
			index.AttributeSet[key] = struct{}{}
		}
		// include additional specified attributes
		if *projection.ProjectionType == "INCLUDE" {
			for _, attribute := range projection.NonKeyAttributes {
				index.AttributeSet[*attribute] = struct{}{}
			}
		}
	}
}
