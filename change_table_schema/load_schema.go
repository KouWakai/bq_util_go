package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"cloud.google.com/go/bigquery"
)

type FieldTypeMapper interface {
	FieldTypeToString(fieldType bigquery.FieldType) string
}

type BigQueryClient interface {
	GetTableMetadata(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.TableMetadata, error)
}

type FieldInfo struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}

type SchemaUseCase struct {
	bqClient     BigQueryClient
	fieldTypeMap FieldTypeMapper
}

func NewSchemaUseCase(bqClient BigQueryClient, fieldTypeMap FieldTypeMapper) *SchemaUseCase {
	return &SchemaUseCase{
		bqClient:     bqClient,
		fieldTypeMap: fieldTypeMap,
	}
}

func (uc *SchemaUseCase) GenerateAndSaveSchema(ctx context.Context, projectID, datasetID, tableID, schemaPath string) error {
	meta, err := uc.bqClient.GetTableMetadata(ctx, projectID, datasetID, tableID)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %v", err)
	}

	var fields []FieldInfo
	for _, field := range meta.Schema {
		fields = append(fields, FieldInfo{
			Name: field.Name,
			Type: uc.fieldTypeMap.FieldTypeToString(field.Type),
		})
	}

	fieldsJSON, err := json.MarshalIndent(fields, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal fields to JSON: %v", err)
	}

	err = ioutil.WriteFile(schemaPath, fieldsJSON, 0644)
	if err != nil {
		return fmt.Errorf("failed to write fields to file: %v", err)
	}

	return nil
}

type FieldTypeMap struct{}

func NewFieldTypeMap() *FieldTypeMap {
	return &FieldTypeMap{}
}

func (fm *FieldTypeMap) FieldTypeToString(fieldType bigquery.FieldType) string {
	switch fieldType {
	case bigquery.StringFieldType:
		return "STRING"
	case bigquery.IntegerFieldType:
		return "INTEGER"
	case bigquery.FloatFieldType:
		return "FLOAT"
	default:
		return "UNKNOWN"
	}
}

type BigQueryClientImpl struct{}

func NewBigQueryClientImpl() *BigQueryClientImpl {
	return &BigQueryClientImpl{}
}

func (c *BigQueryClientImpl) GetTableMetadata(ctx context.Context, projectID string, datasetID string, tableID string) (*bigquery.TableMetadata, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	return client.Dataset(datasetID).Table(tableID).Metadata(ctx)
}

func Load(project_id string, load_datasetid string, load_tableid string, schema_path string) {
	ctx := context.Background()
	projectID := project_id
	datasetID := load_datasetid
	tableID := load_tableid
	schemaPath := schema_path

	fieldTypeMap := NewFieldTypeMap()
	bqClient := NewBigQueryClientImpl()
	useCase := NewSchemaUseCase(bqClient, fieldTypeMap)

	err := useCase.GenerateAndSaveSchema(ctx, projectID, datasetID, tableID, schemaPath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Fields saved to schema.json")
}
