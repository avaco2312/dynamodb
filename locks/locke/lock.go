package locke

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Lock interface {
	Acquire() error
	Release() error
	RemainingDuration() time.Duration
	NewDuration(time.Duration) error
	Fence() string
}

type lock struct {
	fence        string
	table        string
	lockValue    string
	lockType     string
	lockName     string
	startingTime int64
	releaseTime  int64
}

const ZeroDuration time.Duration = 0

func NewLock(svcType string, svc interface{}, table, lockValue, lockType string, duration time.Duration) (Lock, error) {
	var err error
	var lo Lock
	switch svcType {
	case "dynamo":
		lo, err = newDynamoLock(svc.(*dynamodb.DynamoDB), table, lockValue, lockType, duration)
	default:
		return nil, errors.New("error: Unknown lock service")
	}
	if err != nil {
		return nil, err
	}
	return lo, nil
}
