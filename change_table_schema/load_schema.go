package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"cloud.google.com/go/bigquery"
)

func Load(project_id string,load_datasetid string,load_tableid string,schema_path string) {
	// Google Cloudの認証キーファイルへのパスを指定してクライアントを作成します
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

	// スキーマをJSON形式に変換します
	schemaJSON, err := json.MarshalIndent(meta.Schema, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal schema to JSON: %v", err)
	}

	// JSONファイルにスキーマを保存します
	err = ioutil.WriteFile(schema_path, schemaJSON, 0644)
	if err != nil {
		log.Fatalf("Failed to write schema to file: %v", err)
	}

	fmt.Println("Schema saved to schema.json")
}
