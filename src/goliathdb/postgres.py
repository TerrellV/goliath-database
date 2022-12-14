# built in
import logging
import configparser
from pathlib import Path
import os

# local
from goliathdb.config import KEYRING_SERVICE

# 3rd party
import keyring
import boto3
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-6s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %z",
    force=True,
)

s3 = boto3.client("s3")

logger = logging.getLogger(__name__)
config = configparser.ConfigParser()


def download_from_s3(s3uri):
    # s3://gcd-crypto-data/goliath_db_config.ini
    local_config_path = Path().cwd() / "goliath_db_config.ini"
    bucket = s3uri.split("/")[2]
    key = '/'.join(s3uri.split("/")[3:])
    with open(local_config_path, 'wb') as f:
        r = s3.download_fileobj(
            Bucket=bucket,
            Key=key,
            Fileobj=f
        )
    return str(local_config_path)


def load_rds_config():
    """Look for goliath db config ini file"""
    try:
        config_filepath = os.environ["GOLIATH_DB_CONFIG_PATH"]
    except KeyError:
        config_filepath = Path().cwd() / "goliath_db_config.ini"

        if not config_filepath.is_file():
            raise ValueError("unable to load goliath_db_config.ini. Either set the 'GOLIATH_DB_CONFIG_PATH' env variable or place the file in your cwd.")

    # download config from s3
    if str(config_filepath).startswith("s3://"):
        config_filepath = download_from_s3(config_filepath)

    config.read(config_filepath)
    return config["RDS"]


rds_config = load_rds_config()


def get_db_password(username: str, db_config: dict):
    """Retrive temporary password for rds iam authentication."""

    client = boto3.client("rds")
    is_rds_admin = username == db_config["admin_username"]

    if is_rds_admin:
        password = keyring.get_password(KEYRING_SERVICE, "db_pw")
        if password is None:
            logger.error(
                "\n\n\tYou've selected to use the admin rds username and password, but you have not set "
                "a password via keyring.\n\tSet your password as folllows, then rerun."
                "\nimport goliathdb as gd"
                "\ngd.set_password()\n\t\n"
            )
            raise ValueError("Unable to find admin password in keyring")
    else:
        logger.info("Getting temp rds auth token")
        password = client.generate_db_auth_token(
            DBHostname=db_config["host"],
            Port=db_config["port"],
            DBUsername=username,
            Region=db_config["region"],
        )
        logger.info("Token received")

    return password


class PostgresClient:
    def __init__(self, username):

        self.username = username
        self.engine = self._get_engine()
        self.pg_args = {
            # "sslrootcert": os.environ["SSL_ROOT_CERT"],
            # "sslmode": "verify-ca"
        }

    def append(
        self, df: pd.DataFrame, table: str, schema: str = None, chunksize: int = 1000
    ):
        """Append data to existing table in chunks of 1_000.
        Return number of rows inserted.
        """

        df.drop_duplicates(inplace=True)
        rows_inserted = df.to_sql(
            name=table,
            schema=schema,
            con=self.engine,
            if_exists="append",
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        schema = "Public" if schema is None else schema
        logger.info(
            "Appended %s rows to schema = %s, table = %s", rows_inserted, schema, table
        )
        return rows_inserted

    def query(self, sql: str, parse_dates: list = None, **kwargs):
        """Call pandas.read_sql_query()
        Other Supported Args
        --------------------

        index_col : str or list of str, optional, default: None
            Column(s) to set as index(MultiIndex).

        coerce_float : bool, default: True
            Attempts to convert values of non-string, non-numeric objects (like decimal.Decimal)
            to floating point. Useful for SQL result sets.

        params : list, tuple or dict, optional, default: None
            List of parameters to pass to execute method. The syntax used to
            pass parameters is database driver dependent. Check your database driver
            documentation for which of the five syntax styles, described in PEP 249???s
            paramstyle, is supported. Eg. for psycopg2, uses %(name)s so use
            params={???name??? : ???value???}.

        parse_dates : list or dict, default: None
            List of column names to parse as dates.
            Dict of {column_name: format string} where format string is strftime compatible
                in case of parsing string times, or is one of (D, s, ns, ms, us) in case of
                parsing integer timestamps.
            Dict of {column_name: arg dict}, where the arg dict corresponds to the keyword
                arguments of pandas.to_datetime() Especially useful with databases without
                native Datetime support, such as SQLite.

        chunksize : int, default None
            If specified, return an iterator where chunksize is the number of rows to include
            in each chunk.

        dtype: Type name or dict of columns
            Data type for data or columns. E.g. np.float64 or
            {???a???: np.float64, ???b???: np.int32, ???c???: ???Int64???}.
        """
        return pd.read_sql_query(
            sql=sql,
            con=self.engine,
            parse_dates=parse_dates,
            **kwargs,
        )

    def create_view():
        pass

    def create_table():
        pass

    def refresh_materialized_view():
        pass

    def close(self):
        try:
            self.engine.dispose()
        except Exception as err:
            logger.error(err)

    def _get_engine(self):
        """Get sql alchemy engine"""
        logger.info("Creating db engine (using SQLAlchemy + psycopg2)...")

        password = get_db_password(self.username, db_config=rds_config)
        # url_safe_password = urllib.parse.quote_plus(password)
        db_url = sqlalchemy.engine.URL.create(
            drivername="postgresql+psycopg2",
            username=self.username,
            password=password,
            host=rds_config["host"],
            port=rds_config["port"],
            database=rds_config["database"],
        )
        engine = create_engine(
            url=db_url,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
            # connect_args=self.pg_args,
        )

        logger.info("Engine created")

        return engine

    def _execute_sql(self, sql: str):
        """Execute sql statement against postgres rds database"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(sql)
                if result.returns_rows:
                    return result.fetchall()
                return result
        except Exception as err:
            logger.info("Error executing query")
            logger.error(err)
            raise


if __name__ == "__main__":
    pg = PostgresClient(rds_config["admin_username"])
    r = pg._execute_sql(
        """
        select *
        from market.trading_pairs
        """
    )
    print(r)
    pg.close()
