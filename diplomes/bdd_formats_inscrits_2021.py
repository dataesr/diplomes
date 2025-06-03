#!/usr/bin/env python
# coding: utf-8


"""
This program places formats inscrits into db

"""

import sqlite3
from sqlite3 import Error
import os
import pandas as pd
import pyreadstat
from diplomes import dtypes_inputs as typesd

from application.server.main.logger import get_logger

logger = get_logger(__name__)

pd.options.mode.chained_assignment = None

DATA_PATH = "/run/media/julia/DATA/diplomes_donnees/"

os.chdir(DATA_PATH)

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    co = None
    try:
        co = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return co


def create_db_result(df: pd.DataFrame, nom: str):
    """
    Read pseudo ID generated previously, associate them with actual building ID and load the result into a SQLite db.

    """
    os.chdir(DATA_PATH)

    conn = create_connection("resutl21.db")

    df.to_sql(nom, conn, if_exists="replace", index=False)

    if conn:
        conn.close()
