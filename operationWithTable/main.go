package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
			TableName:   aws.String("Artistas"),
			BillingMode: types.BillingModePayPerRequest,
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("Artist"),
					KeyType:       types.KeyTypeHash, //Partition key
				},
				{
					AttributeName: aws.String("Movie"),
					KeyType:       types.KeyTypeRange, //Sort key
				},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("Artist"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("Movie"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
		},
	)
	if err != nil {
		return fmt.Errorf("error: Creando tabla AWS %v", err)
	}
	return nil
}

func CreateSecIndex(ctx context.Context, cfg aws.Config) error {
	svc := dynamodb.NewFromConfig(cfg)
	_, err := svc.UpdateTable(ctx,
		&dynamodb.UpdateTableInput{
			TableName: aws.String("Artistas"),

			GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
				{
					Create: &types.CreateGlobalSecondaryIndexAction{
						IndexName: aws.String("Movies"),
						Projection: &types.Projection{
							ProjectionType: types.ProjectionTypeAll,
						},
						KeySchema: []types.KeySchemaElement{
							{
								AttributeName: aws.String("Movie"),
								KeyType:       types.KeyTypeHash, //Partition key
							},
							{
								AttributeName: aws.String("Artist"),
								KeyType:       types.KeyTypeRange, //Sort key
							},
						},
					},
				},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("Movie"),
					AttributeType: types.ScalarAttributeTypeS,
				},
				{
					AttributeName: aws.String("Artist"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
		})
	if err != nil {
		return fmt.Errorf("error: Creando secondary index AWS %v", err)
	}
	return nil
}

func DeleteTable(ctx context.Context, cfg aws.Config) error {
	svc := dynamodb.NewFromConfig(cfg)
	_, err := svc.DeleteTable(ctx,
		&dynamodb.DeleteTableInput{
			TableName: aws.String("Artistas"),
		},
	)
	return err
}

func LoadTable(ctx context.Context, cfg aws.Config) error {
	f, err := ioutil.ReadFile("artist.json")
	if err != nil {
		return fmt.Errorf("failed to read file, %w", err)
	}
	data := []ArtistS{}
	err = json.Unmarshal(f, &data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal Record, %w", err)
	}
	for _, at := range data {
		for _, mv := range at.Movies {
			mv.Artist = at.Artist
			av, err := attributevalue.MarshalMap(mv)
			if err != nil {
				return fmt.Errorf("failed to marshal Record, %w", err)
			}
			svc := dynamodb.NewFromConfig(cfg)
			_, err = svc.PutItem(ctx,
				&dynamodb.PutItemInput{
					TableName: aws.String("Artistas"),
					Item:      av,
				})
			if err != nil {
				return fmt.Errorf("failed to Putitem, %w", err)
			}
		}
	}
	return nil
}

func QueryTable(ctx context.Context, cfg aws.Config) error {
	svc := dynamodb.NewFromConfig(cfg)
	qo, err := svc.Query(ctx,
		&dynamodb.QueryInput{
			TableName:                aws.String("Artistas"),
			KeyConditionExpression:   aws.String("#artist = :hashKey"),
			FilterExpression:         aws.String("#genre = :rangeKey"),
			ExpressionAttributeNames: map[string]string{"#artist": "Artist", "#genre": "Genre"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":hashKey":  &types.AttributeValueMemberS{Value: "Tom Hanks"},
				":rangeKey": &types.AttributeValueMemberS{Value: "Drama"},
			},
		})
	if err != nil {
		return fmt.Errorf("failed to Query, %w", err)
	}
	data := []MoviesS{}
	err = attributevalue.UnmarshalListOfMaps(qo.Items, &data)
	if err != nil {
		return fmt.Errorf("failed to Unmarshal, %w", err)
	}
	for _, mv := range data {
		fmt.Println(mv)
	}
	fmt.Println()
	qo, err = svc.Query(ctx,
		&dynamodb.QueryInput{
			TableName:                aws.String("Artistas"),
			KeyConditionExpression:   aws.String("#artist = :hashKey"),
			ExpressionAttributeNames: map[string]string{"#artist": "Artist"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":hashKey": &types.AttributeValueMemberS{Value: "Tom Hanks"},
			},
		})
	if err != nil {
		return fmt.Errorf("failed to Query, %w", err)
	}
	data = []MoviesS{}
	err = attributevalue.UnmarshalListOfMaps(qo.Items, &data)
	if err != nil {
		return fmt.Errorf("failed to Unmarshal, %w", err)
	}
	for _, mv := range data {
		fmt.Println(mv)
	}
	return nil
}

func QueryIndex(ctx context.Context, cfg aws.Config) error {
	svc := dynamodb.NewFromConfig(cfg)
	qo, err := svc.Query(ctx,
		&dynamodb.QueryInput{
			IndexName:                aws.String("Movies"),
			TableName:                aws.String("Artistas"),
			KeyConditionExpression:   aws.String("#movie = :hashKey AND #artist = :rangeKey"),
			ExpressionAttributeNames: map[string]string{"#artist": "Artist", "#movie": "Movie"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":hashKey":  &types.AttributeValueMemberS{Value: "Toy Story"},
				":rangeKey": &types.AttributeValueMemberS{Value: "Tom Hanks"},
			},
		})
	if err != nil {
		return fmt.Errorf("failed to Query, %w", err)
	}
	data := []MoviesS{}
	err = attributevalue.UnmarshalListOfMaps(qo.Items, &data)
	if err != nil {
		return fmt.Errorf("failed to Unmarshal, %w", err)
	}
	for _, mv := range data {
		fmt.Println(mv)
	}
	fmt.Println()
	qo, err = svc.Query(ctx,
		&dynamodb.QueryInput{
			IndexName:                aws.String("Movies"),
			TableName:                aws.String("Artistas"),
			KeyConditionExpression:   aws.String("#movie = :hashKey"),
			ExpressionAttributeNames: map[string]string{"#movie": "Movie"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":hashKey": &types.AttributeValueMemberS{Value: "Toy Story"},
			},
		})
	if err != nil {
		return fmt.Errorf("failed to Query, %w", err)
	}
	data = []MoviesS{}
	err = attributevalue.UnmarshalListOfMaps(qo.Items, &data)
	if err != nil {
		return fmt.Errorf("failed to Unmarshal, %w", err)
	}
	for _, mv := range data {
		fmt.Println(mv)
	}
	return nil
}

func main() {
	ct := flag.Bool("c", false, "Create table")
	si := flag.Bool("s", false, "Create global secondary index")
	lt := flag.Bool("l", false, "Load table")
	qt := flag.Bool("t", false, "Query table")
	qi := flag.Bool("i", false, "Query index")
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
	if *si {
		if err := CreateSecIndex(ctx, cfg); err != nil {
			fmt.Printf(err.Error())
		}
	}
	if *lt {
		if err := LoadTable(ctx, cfg); err != nil {
			fmt.Printf(err.Error())
		}
	}
	if *qi {
		if err := QueryIndex(ctx, cfg); err != nil {
			fmt.Printf(err.Error())
		}
		fmt.Println()
	}
	if *qt {
		if err := QueryTable(ctx, cfg); err != nil {
			fmt.Printf(err.Error())
		}
	}
	if *dt {
		if err := DeleteTable(ctx, cfg); err != nil {
			fmt.Printf(err.Error())
		}
	}
}
