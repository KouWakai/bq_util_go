package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type Config struct {
	ProjectID     string   `json:"project_id"`
	DatasetIDs    []string `json:"dataset_ids"`
	TableName     string   `json:"tableName"`
	TargetDataset string   `json:"targetDataset"`
}

type TableRow struct {
	No                 string
	datasetid          string
	tablename          string
	last_modified_date string
}

func main() {
	config := readConfig("env.json")
	projectID := config.ProjectID
	datasetIDs := config.DatasetIDs

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	for _, datasetID := range datasetIDs {
		rows := []*TableRow{}

		tables := client.Dataset(datasetID).Tables(ctx)
		for {
			table, err := tables.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Printf("Failed to iterate tables in dataset %s: %v", datasetID, err)
				break
			}

			tableMeta, err := table.Metadata(ctx)
			if err != nil {
				log.Printf("Failed to get metadata for table %s: %v", table.TableID, err)
				continue
			}
			layout := "2006-01-02 15:04:05"
			lastModifiedTime := tableMeta.LastModifiedTime

			row := &TableRow{
				No:                 strconv.Itoa(len(rows) + 1),
				datasetid:          datasetID,
				tablename:          table.TableID,
				last_modified_date: lastModifiedTime.Format(layout),
			}
			rows = append(rows, row)
		}

		tableName := config.TableName
		targetDataset := config.TargetDataset
		if err := writeRowsToBigQuery(ctx, client, projectID, targetDataset, tableName, rows); err != nil {
			log.Printf("Failed to write rows to BigQuery table in dataset %s: %v", targetDataset, err)
		}
	}
}

func readConfig(filename string) Config {
	configFile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open config file: %v", err)
	}
	defer configFile.Close()

	var config Config
	if err := json.NewDecoder(configFile).Decode(&config); err != nil {
		log.Fatalf("Failed to decode config JSON: %v", err)
	}

	return config
}

type Item struct {
	No                 string `bigquery:"No"`
	datasetid          string `bigquery:"datasetid"`
	tablename          string `bigquery:"tablename"`
	last_modified_date string `bigquery:"last_modified_date"`
}

func writeRowsToBigQuery(ctx context.Context, client *bigquery.Client, projectID, datasetID, tableName string, rows []*TableRow) error {
	uploader := client.Dataset(datasetID).Table(tableName).Uploader()

	items := make([]*Item, len(rows))
	for i, row := range rows {
		item := &Item{
			No:                 row.No,
			datasetid:          row.datasetid,
			tablename:          row.tablename,
			last_modified_date: row.last_modified_date,
		}
		items[i] = item
	}

	// Convert []*Item to []bigquery.ValueSaver using Save method
	valueSavers := make([]bigquery.ValueSaver, len(items))
	for i, item := range items {
		valueSavers[i] = item
	}

	if err := uploader.Put(ctx, valueSavers); err != nil {
		return fmt.Errorf("error uploading rows: %v", err)
	}
	return nil
}

// Save implements the ValueSaver interface.
func (i *Item) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"No":                 i.No,
		"datasetid":          i.datasetid,
		"tablename":          i.tablename,
		"last_modified_date": i.last_modified_date,
	}, bigquery.NoDedupeID, nil
}
