package dynamodbfriend

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Put puts an item into the table. The item should have all attributes to be included in the
// table item tagged with the "dynamodbav" struct tag.
func (table *Table) Put(ctx context.Context, item interface{}) error {
	attrMap, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return err
	}

	_, err = table.baseClient.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: &table.Name,
		Item:      attrMap,
	})

	return err
}
