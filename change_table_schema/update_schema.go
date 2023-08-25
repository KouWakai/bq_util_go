package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"cloud.google.com/go/bigquery"
)

func fieldTypeToString(ft bigquery.FieldType) string {
	switch ft {
	case bigquery.StringFieldType:
		return "STRING"
	case bigquery.BytesFieldType:
		return "BYTES"
	case bigquery.IntegerFieldType:
		return "INTEGER"
	case bigquery.FloatFieldType:
		return "FLOAT"
	case bigquery.BooleanFieldType:
		return "BOOLEAN"
	case bigquery.TimestampFieldType:
		return "TIMESTAMP"
	case bigquery.NumericFieldType:
		return "NUMERIC"
	case bigquery.RecordFieldType:
		return "RECORD"
	case bigquery.DateFieldType:
		return "DATE"
	case bigquery.TimeFieldType:
		return "TIME"
	case bigquery.DateTimeFieldType:
		return "DATETIME"
	case bigquery.GeographyFieldType:
		return "GEOGRAPHY"
	default:
		return "UNKNOWN"
	}
}

func Update(project_id string, load_datasetid string, load_tableid string, schema_path string) {
	// Google Cloudのクライアントを作成します
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, project_id)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// スキーマを取得するテーブルの情報を指定します
	datasetID := load_datasetid
	tableID := load_tableid

	// テーブルのメタデータを取得します
	meta, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		log.Fatalf("Failed to get table metadata: %v", err)
	}

	// JSONファイルからスキーマを読み込みます
	jsonSchema, err := ioutil.ReadFile(schema_path)
	if err != nil {
		log.Fatalf("Failed to read schema file: %v", err)
	}

	// スキーマのフィールドリストを取得します
	var newSchema bigquery.Schema
	err = json.Unmarshal(jsonSchema, &newSchema)
	if err != nil {
		log.Fatalf("Failed to unmarshal schema from JSON: %v", err)
	}

	// 既存のフィールドを保持するためのスライスを作成します
	existingFields := make(map[string]bool)
	for _, field := range meta.Schema {
		existingFields[field.Name] = true
	}

	// 新しいフィールドが既存のスキーマに含まれているかチェックし、存在しない場合は空白の初期値のフィールドを追加します
	var newFieldNames []string
	for _, newField := range newSchema {
		if !existingFields[newField.Name] {
			newField.Description = "New field"
			newField.Required = false
			insert(client, datasetID, tableID, newField.Name)
		}
		newFieldNames = append(newFieldNames, newField.Name)
	}

	// 既存のテーブルを置き換えるためのCREATE OR REPLACE文を生成します
	createTableStmt := fmt.Sprintf("CREATE OR REPLACE TABLE `%s.%s` AS SELECT %s FROM `%s.%s`", datasetID, tableID, strings.Join(newFieldNames, ", "), datasetID, tableID)
	// テーブルを置き換えます
	query := client.Query(createTableStmt)
	_, err = query.Run(ctx)
	if err != nil {
		log.Fatalf("Failed to replace table: %v", err)
	}

	fmt.Println("Table replaced successfully")
}

func insert(client *bigquery.Client, datasetID, tableID, newColumn string) {
	ctx := context.Background()
	tableRef := client.Dataset(datasetID).Table(tableID)

	// テーブルメタデータ取得
	tableMeta, err := tableRef.Metadata(ctx)
	if err != nil {
		log.Fatalf("Failed to get table metadata: %v", err)
	}

	// 新しいカラムでフィールドを作成
	newField := &bigquery.FieldSchema{
		Name: newColumn,
		Type: bigquery.StringFieldType,
	}

	// スキーマにカラムを追加
	tableMeta.Schema = append(tableMeta.Schema, newField)

	// テーブルメタデータを作成
	tableMetaToUpdate := &bigquery.TableMetadataToUpdate{
		Schema: tableMeta.Schema,
	}

	// 作成したテーブルメタ情報で更新
	if _, err := tableRef.Update(ctx, *tableMetaToUpdate, ""); err != nil {
		log.Fatalf("Failed to update table schema: %v", err)
	}

	fmt.Println("Column added successfully!")
}
