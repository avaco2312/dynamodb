package main

import (
	"dynamodb/locks/locke"
	"errors"
	"flag"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	lockTable = "LockTable"
)

var svc *dynamodb.DynamoDB

func init() {
	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000")})
	if err != nil {
		log.Fatal(err)
	}
	svc = dynamodb.New(sess)
}

func CreateTable() error {
	_, err := svc.CreateTable(
		&dynamodb.CreateTableInput{
			TableName:   aws.String(lockTable),
			BillingMode: aws.String("PAY_PER_REQUEST"),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("tabla"),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String("lockvalue"),
					KeyType:       aws.String("RANGE"),
				},
			},
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("tabla"),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String("lockvalue"),
					AttributeType: aws.String("S"),
				},
			},
		})
	if err != nil {
		return err
	}
	err = svc.WaitUntilTableExists(
		&dynamodb.DescribeTableInput{
			TableName: aws.String(lockTable),
		})
	if err != nil {
		return errors.New("error: timed out while waiting for table to become active")
	}
	_, err = svc.UpdateTimeToLive(
		&dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(lockTable),
			TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
				AttributeName: aws.String("releasetime"),
				Enabled:       aws.Bool(true),
			},
		},
	)
	if err != nil {
		return errors.New("error: failed enable TTL")
	}
	_, err = svc.UpdateItem(
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
				":zero": {N: aws.String("0")},
			},
			UpdateExpression: aws.String(
				"SET #fence = :zero",
			),
		},
	)
	return err
}

func DeleteTable() error {
	_, err := svc.DeleteTable(
		&dynamodb.DeleteTableInput{
			TableName: aws.String(lockTable),
		},
	)
	return err
}

func main() {
	ct := flag.Bool("c", false, "Create table")
	el := flag.Bool("l", false, "Lock")
	dt := flag.Bool("d", false, "Delete table")
	flag.Parse()
	if *ct {
		if err := CreateTable(); err != nil {
			log.Fatal(err)
		}
	}
	if *el {

		log.Println("Lock1")
		Lock1, _ := locke.NewLock("dynamo", svc, "Usuarios", "Pepe", "Lock1", 3*time.Minute)
		err := Lock1.Acquire()
		log.Println(err)
		log.Println(Lock1.Fence())
		log.Println(Lock1.RemainingDuration())
		log.Println()

		log.Println("Change duracion Lock1")
		err = Lock1.NewDuration(5 * time.Minute)
		log.Println(err)
		log.Println(Lock1.Fence())
		log.Println(Lock1.RemainingDuration())
		log.Println()

		log.Println("Lock2")
		Lock2, _ := locke.NewLock("dynamo", svc, "Usuarios", "Pepe", "Lock2", 3*time.Minute)
		err = Lock2.Acquire()
		log.Println(err)
		log.Println(Lock2.Fence())
		log.Println(Lock2.RemainingDuration())
		log.Println()

		log.Println("UnLock1")
		err = Lock1.Release()
		log.Println(err)
		log.Println(Lock1.Fence())
		log.Println(Lock1.RemainingDuration())
		log.Println()

		log.Println("Lock3")
		Lock3, _ := locke.NewLock("dynamo", svc, "Usuarios", "Pepe", "Lock3", 4*time.Minute)
		err = Lock3.Acquire()
		log.Println(err)
		log.Println(Lock3.Fence())
		log.Println(Lock3.RemainingDuration())

	}
	if *dt {
		if err := DeleteTable(); err != nil {
			log.Fatal(err)
		}
	}
}
