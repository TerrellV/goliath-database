# Goliath Database

## Installation

```bash
pip install git@github.com:TerrellV/goliath-database.git
```

## Usage

```py
>>> import goliathdb as gd
>>> pg = gd.PostgresClient(username="jadyn")

# write new data to rds table; do nothing on conflict; returns number of rows written to database
>>> pg.append(df, table="movies", schema="entertainment")
10

# execute select statements and returns Pandas.DataFrame
sql = """
SELECT *
FROM entertainment.movies
ORDER BY release_date DESC
LIMIT 2
"""

>>> pg.query(sql)
"""
release_date | title
------------ | --------------
1997-12-19   | Titanic
1993-06-11   | Jurrasic Park
"""
```
