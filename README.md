# Goliath Database

## Installation

```bash
pip install git+https://github.com/TerrellV/goliath-database.git
```

## Defining db config file

This package requires a goliath_db_config.ini file to run. It will search for that file in the following order:

* load file using path specified in the environment variable 'GOLIATH_DB_CONFIG_PATH'
  * file path can be a string to a local file path or valid s3 uri
* if environment variable not set, it will search for goliath_db_config.ini in your current working directory

## Requirement for using root db admin account

```py
>>> import goliathdb as gd
>>> gd.set_password()
>>> RDS Password:
```

## General Usage

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
