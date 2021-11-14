package dynamodbfriend

// Logger is an interface used by dynamodbfriend for all logging.
type Logger interface {
	Printf(format string, v ...interface{})
}

type nullLogger struct{}

func (l nullLogger) Printf(_ string, _ ...interface{}) {}
