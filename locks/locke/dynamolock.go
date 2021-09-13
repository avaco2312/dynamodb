package locke

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	UploadTransaction = "UploadTransaction"
	lockTable         = "LockTable"
)

type dynamolock struct {
	lock
	svc *dynamodb.DynamoDB
}

func newDynamoLock(svc *dynamodb.DynamoDB, table, lockValue, lockType string, duration time.Duration) (Lock, error) {
	now := time.Now().UTC()
	return &dynamolock{
		svc: svc,
		lock: lock{
			fence:        "0",
			table:        table,
			lockValue:    lockValue,
			lockType:     lockType,
			lockName:     strings.Join([]string{lockValue, lockType}, "->"),
			startingTime: now.Unix(),
			releaseTime:  now.Add(duration).Unix(),
		},
	}, nil
}

func (l *dynamolock) Acquire() error {
	now := time.Now().UTC().Unix()
	// Ya vencio el lock
	if l.releaseTime <= now {
		l.fence = "0"
		return errors.New("error: not creating an expired lock")
	}
	// Ya tiene adquirido el lock. Pregunta: Así, no error, o devolver error?
	if l.fence != "0" {
		return nil
	}
	// Obtener del fencing global e incrementarlo
	uio, err := l.svc.UpdateItem(
		&dynamodb.UpdateItemInput{
			TableName: aws.String(lockTable),
			Key: map[string]*dynamodb.AttributeValue{
				"tabla":     {S: aws.String(lockTable)},
				"lockvalue": {S: aws.String(lockTable)},
			},
			ExpressionAttributeNames: map[string]*string{
				"#fence": aws.String("fence"),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":uno": {N: aws.String("1")},
			},
			UpdateExpression: aws.String(
				"SET #fence = #fence + :uno",
			),
			ReturnValues: aws.String("UPDATED_NEW"),
		},
	)
	if err != nil {
		return err
	}
	fence := uio.Attributes["fence"].N
	// Obtener el lock
	_, err = l.svc.UpdateItem(
		&dynamodb.UpdateItemInput{
			TableName: aws.String(lockTable),
			Key: map[string]*dynamodb.AttributeValue{
				"tabla":     {S: aws.String(l.table)},
				"lockvalue": {S: aws.String(l.lockValue)},
			},
			ConditionExpression: aws.String(
				"attribute_not_exists(#tabla) OR " +
					"#fence = :zero OR " +
					"#releasetime < :now",
			),
			ExpressionAttributeNames: map[string]*string{
				"#tabla":        aws.String("tabla"),
				"#releasetime":  aws.String("releasetime"),
				"#fence":        aws.String("fence"),
				"#lockname":     aws.String("lockname"),
				"#locktype":     aws.String("locktype"),
				"#startingtime": aws.String("startingTime"),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":zero":         {N: aws.String("0")},
				":fence":        {N: fence},
				":now":          {N: aws.String(strconv.FormatInt(now, 10))},
				":releasetime":  {N: aws.String(strconv.FormatInt(l.releaseTime, 10))},
				":lockname":     {S: aws.String(l.lockName)},
				":locktype":     {S: aws.String(l.lockType)},
				":startingtime": {S: aws.String(strconv.FormatInt(l.startingTime, 10))},
			},
			UpdateExpression: aws.String(
				"SET #releasetime = :releasetime, #fence = :fence, #lockname = :lockname, " +
					"#locktype = :locktype, #startingtime = :startingtime",
			),
		},
	)
	// Si se obtuvo el lock registrar el fence
	if err == nil {
		l.fence = *fence
	}
	return err

}

func (l *dynamolock) NewDuration(duration time.Duration) error {
	// Lock no adquirido no se puede cambiar duracion
	if l.fence == "0" {
		return errors.New("error: Lock no adquired, no new duration")
	}
	now := time.Now().UTC()
	// Lock expirado no se puede cambiar duracion
	if l.releaseTime <= now.Unix() {
		l.fence = "0"
		return errors.New("error: not creating an expired lock")
	}
	nrt := now.Add(duration).Unix()
	_, err := l.svc.UpdateItem(
		&dynamodb.UpdateItemInput{
			TableName: aws.String(lockTable),
			Key: map[string]*dynamodb.AttributeValue{
				"tabla":     {S: aws.String(l.table)},
				"lockvalue": {S: aws.String(l.lockValue)},
			},
			ConditionExpression: aws.String(
				"#fence > :zero AND " +
					"#releasetime > :now",
			),
			ExpressionAttributeNames: map[string]*string{
				"#fence":       aws.String("fence"),
				"#releasetime": aws.String("releasetime"),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{

				":zero": {N: aws.String("0")},
				":nrt":  {N: aws.String(strconv.FormatInt(nrt, 10))},
				":now":  {N: aws.String(strconv.FormatInt(now.Unix(), 10))},
			},
			UpdateExpression: aws.String(
				"SET #releasetime = :nrt",
			),
		},
	)
	if err == nil {
		l.releaseTime = nrt
	}
	return err
}

func (l *dynamolock) Release() error {
	// Lock no adquirido no se puede hacer release
	if l.fence == "0" {
		return errors.New("error: lock no adquirido")
	}
	now := time.Now().UTC().Unix()
	// Lock expirado no se puede hacer release
	// No es necesario ir a la base de datos, "confiamos" en el reloj de lambda y
	// los problemas se evitan mediante fencing
	if l.releaseTime <= now {
		l.fence = "0"
		return errors.New("error: lock expirado")
	}
	_, err := l.svc.UpdateItem(
		&dynamodb.UpdateItemInput{
			TableName: aws.String(lockTable),
			Key: map[string]*dynamodb.AttributeValue{
				"tabla":     {S: aws.String(l.table)},
				"lockvalue": {S: aws.String(l.lockValue)},
			},
			ConditionExpression: aws.String(
				"#fence > :zero AND " +
					"#releasetime > :now",
			),
			ExpressionAttributeNames: map[string]*string{
				"#fence":       aws.String("fence"),
				"#releasetime": aws.String("releasetime"),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":zero": {N: aws.String("0")},
				":now":  {N: aws.String(strconv.FormatInt(now, 10))},
			},
			UpdateExpression: aws.String(
				"SET #fence = :zero",
			),
		},
	)
	if err == nil {
		l.fence = "0"
	}
	return err
}

func (l *lock) RemainingDuration() time.Duration {
	// Lock no aquirido
	if l.fence == "0" {
		return ZeroDuration
	}
	now := time.Now().UTC().Unix()
	// Lock expirado
	if l.releaseTime <= now {
		l.fence = "0"
		return ZeroDuration
	}
	// Calcular tiempo restante en segundos
	return time.Duration((l.releaseTime - now) * 1000000000)
}

func (l *lock) Fence() string {
	now := time.Now().UTC().Unix()
	// Lock expirado, cuando se vaya escribir en la base de datos
	// le evita chequear la consistencia mediante el fencing
	if l.releaseTime <= now {
		l.fence = "0"
	}
	return l.fence
}



