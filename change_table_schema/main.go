package main

import (
    "os"
	"github.com/alecthomas/kingpin/v2"
)

var (
    // アプリケーションの定義
    app      = kingpin.New("update_table_schema", "A CLI for manipulate bigquery table")

    // loadコマンドの定義
    load     = app.Command("load", "load schema of an existing table determine by args")
    load_datasetid = load.Arg("datasetid", "dataset id of a table to load its schema").Required().String()
    load_tableid = load.Arg("tableid", "table id of a table to load its schema").Required().String()
	load_schema_path = load.Arg("schemapath","schema path to json").Required().String()
	load_project_id  =load.Arg("projectid","project id which the table is belong to").Required().String()

    // updateコマンドの定義
    update        = app.Command("update", "update schema of an existing table")
    update_datasetid = update.Arg("datasetid", "dataset id of a table to update its schema").Required().String()
    update_tableid = update.Arg("tableid", "table id of a table to update its schema").Required().String()
	update_schema_path = update.Arg("schemapath","schema path to json").Required().String()
	update_project_id  =update.Arg("projectid","project id which the table is belong to").Required().String()
)

func main() {
    // コマンドライン引数をパース
    switch kingpin.MustParse(app.Parse(os.Args[1:])) {
    case load.FullCommand():
        // load コマンドの実行
		Load(*load_project_id,*load_datasetid,*load_tableid,*load_schema_path)
    case update.FullCommand():
        // update コマンドの実行
		Update(*update_project_id,*load_datasetid,*load_tableid,*update_schema_path)
    }
}
