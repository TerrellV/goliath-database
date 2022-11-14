import keyring
from getpass import getpass
from goliathdb.config import KEYRING_SERVICE

def set_password():
    keyring.set_password(KEYRING_SERVICE, "db_pw", getpass("RDS Password:"))