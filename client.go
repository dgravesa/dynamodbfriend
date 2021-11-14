package dynamodbfriend

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Client is a high-level client to DynamoDB. Client is a wrapper around a DynamoDB interface
// implementer, such as dynamodb.DynamoDB, that enables high-level functionality provided by this
// package.
type Client struct {
	Base dynamodbiface.DynamoDBAPI
}

// NewClient creates a new Client instance from a regular DynamoDB client from the AWS SDK v1 for Go.
func NewClient(dynamoDB dynamodbiface.DynamoDBAPI) *Client {
	return &Client{Base: dynamoDB}
}
