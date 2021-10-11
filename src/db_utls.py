import sqlite3
from sqlite3 import Error
import os
from os.path import expanduser
import sqlalchemy
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Integer, String

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except Error as e:
        print(e)
    


if __name__ == '__main__':
    engine = create_engine('sqlite:///Users/vkrishnan/software/psrpype-uwl/psrpype.db', echo=True)