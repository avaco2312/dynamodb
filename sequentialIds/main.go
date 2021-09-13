package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	artistas  = []string{"Tom Hanks", "Natalie Portman", "Marlon Brando"}
	peliculas = [][]string{{"Toy Story", "Forrest Gump", "Catch Me If You Can"}, {"Black Swan", "V for Vendetta"}, {"The Godfather", "Apocalipsis Now"}}
)

type rep struct{}

func (r rep) ResolveEndpoint(service string, region string) (aws.Endpoint, error) {
	return aws.Endpoint{
		URL: "http://localhost:8000",
	}, nil
}

type MoviesS struct {
	Artist string
	Movie  string
	Role   string
	Year   string
	Genre  string
}

type ArtistS struct {
	Artist string
	Movies []MoviesS
}

func CreateConfig(ctx context.Context) (aws.Config, error) {
	res := rep{}
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		func(v *config.LoadOptions) error {
			v.EndpointResolver = res
			return nil
		})
	if err != nil {
		return aws.Config{}, fmt.Errorf("error: Cargando configuracion AWS %v", err)
	}
	return cfg, nil
}

func CreateTable(ctx context.Context, cfg aws.Config) error {
	svc := dynamodb.NewFromConfig(cfg)
	_, err := svc.CreateTable(ctx,
		&dynamodb.CreateTableInput{
			TableName:   aws.String("JiraTable"),
			BillingMode: types.BillingModePayPerRequest,
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("PK"),
					KeyType:       types.KeyTypeHash, //Partition key
				},
				{
					AttributeName: aws.String("SK"),
					KeyType:       types.KeyTypeRange, //Sort key
				},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("PK"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("SK"),
					AttributeType: types.ScalarAttributeTypeN,
				},
			},
		},
	)
	if err != nil {
		return fmt.Errorf("error: Creando tabla AWS %v", err)
	}
	return nil
}

func DeleteTable(ctx context.Context, cfg aws.Config) error {
	svc := dynamodb.NewFromConfig(cfg)
	_, err := svc.DeleteTable(ctx,
		&dynamodb.DeleteTableInput{
			TableName: aws.String("JiraTable"),
		},
	)
	return err
}

func PutItem(ctx context.Context, cfg aws.Config, artista, pelicula string) error {
	svc := dynamodb.NewFromConfig(cfg)
	// If PK is new create PK y SK 0
	_, err := svc.PutItem(ctx,
		&dynamodb.PutItemInput{
			TableName: aws.String("JiraTable"),
			Item: map[string]types.AttributeValue{
				"PK":    &types.AttributeValueMemberS{Value: artista},
				"SK":    &types.AttributeValueMemberN{Value: "0"},
				"Count": &types.AttributeValueMemberN{Value: "0"},
			},
			ConditionExpression:      aws.String("attribute_not_exists(#seq)"),
			ExpressionAttributeNames: map[string]string{"#seq": "SK"},
		})
	if err != nil {
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			if ccfe.ErrorCode() != "ConditionalCheckFailedException" {
				return fmt.Errorf("failed to Putitem, %v", err)
			}
		}
	}
	nint := 0
	for {
		gio, err := svc.GetItem(ctx,
			&dynamodb.GetItemInput{
				TableName: aws.String("JiraTable"),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: artista},
					"SK": &types.AttributeValueMemberN{Value: "0"},
				},
				ProjectionExpression:     aws.String("#count"),
				ExpressionAttributeNames: map[string]string{"#count": "Count"},
			})
		if err != nil {
			return fmt.Errorf("failed to Get seq, %w", err)
		}
		var count int
		err = attributevalue.Unmarshal(gio.Item["Count"], &count)
		if err != nil {
			return fmt.Errorf("failed to Unmarshal, %w", err)
		}
		_, err = svc.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Put: &types.Put{
						Item: map[string]types.AttributeValue{
							"PK":     &types.AttributeValueMemberS{Value: artista},
							"SK":     &types.AttributeValueMemberN{Value: fmt.Sprint(count + 1)},
							"Nombre": &types.AttributeValueMemberS{Value: pelicula},
						},
						TableName: aws.String("JiraTable"),
					},
				},
				{
					Update: &types.Update{
						ConditionExpression: aws.String("#count = :count"),
						ExpressionAttributeNames: map[string]string{
							"#count": "Count",
						},
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":uno":   &types.AttributeValueMemberN{Value: "1"},
							":count": &types.AttributeValueMemberN{Value: fmt.Sprint(count)},
						},
						Key: map[string]types.AttributeValue{
							"PK": &types.AttributeValueMemberS{Value: artista},
							"SK": &types.AttributeValueMemberN{Value: "0"},
						},
						TableName:        aws.String("JiraTable"),
						UpdateExpression: aws.String("SET #count = #count + :uno"),
					},
				},
			},
		})
		if err == nil {
			return nil
		} else {
			nint++
			if nint == 3 {
				return err
			}
		}
	}
}

func main() {
	ct := flag.Bool("c", false, "Create table")
	pi := flag.Bool("p", false, "PutItems")
	dt := flag.Bool("d", false, "Delete table")
	flag.Parse()
	ctx := context.TODO()
	cfg, err := CreateConfig(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	if *ct {
		if err := CreateTable(ctx, cfg); err != nil {
			fmt.Printf(err.Error())
		}
	}
	if *pi {
		now := time.Now()
		var wg sync.WaitGroup
		for i, ar := range artistas {
			for _, mv := range peliculas[i] {
				ar := ar
				mv := mv
				wg.Add(1)
				go func() {
					if err := PutItem(ctx, cfg, ar, mv); err != nil {
						fmt.Printf(err.Error())
					}
					wg.Done()
				}()
			}
		}
		wg.Wait()
		fmt.Printf("Duración %v", time.Since(now))
	}
	if *dt {
		if err := DeleteTable(ctx, cfg); err != nil {
			fmt.Printf(err.Error())
		}
	}
}
