# bq_util_go

### update_table_info
tablemetaよりmodified_dateを取得しbigqueryテーブルにupsertする。
loockerでテーブルの更新情報を一覧で確認するために使用している。

### change_table_schema
既存のテーブルのスキーマをjsonに書き出し、jsonをもとにテーブルのスキーマを更新するCLIツール。

# モチベーション
BigQueryのwebUIではカラムの変更が容易にできないため、CLIで簡単に行得るようにしたくて作成した。
