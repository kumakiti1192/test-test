# Test Repository

This repository contains a sample script `keiba_analysis.py` for analyzing horse racing predictions from Excel files. The script first tries to download race results via HTTP requests and, if that fails, falls back to using a headless Chrome browser. Fetched HTML is cached under `cache/` so repeated runs do not require network access. It then generates betting tickets and calculates statistics such as hit rate and return on investment.

## セットアップ

1. Python 3.11 での動作を想定しています。
2. 依存パッケージをインストールします。

   ```bash
   pip install -r requirements.txt
   ```

## 実行方法

`予想データ` ディレクトリに対象の Excel ファイル（`YYYYMMDD_*.xlsx` 形式）を配置してから次のコマンドを実行します。

```bash
python keiba_analysis.py
```

`LOGLEVEL` 環境変数に `DEBUG` を指定すると、より詳細なログを出力します。

取得した結果ページの HTML は `cache/results` 以下に保存されます。すでに
キャッシュが存在する場合はネットワークアクセスなしで再利用されます。
