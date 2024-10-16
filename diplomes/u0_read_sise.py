import pandas as pd
import os
# from application.server.main.logger import get_logger
from diplomes import dtypes_inputs as typesd

# logger = get_logger(__name__)

# DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")
DATA_PATH = "/run/media/julia/DATA/diplomes/"


def get_filename(source, an):
    if source == "result":
        filename = f'{source}{an}_ssa.parquet'
    else:
        filename = f'dip{source}{an}_ssa.parquet'
    return filename


def annee(df: pd.DataFrame):
    col_an = ["ANBAC", "ANNAIS", "ANINSC", "ACARESPA", "DEPRESPA"]
    for col in col_an:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df.loc[df[col].notna(), col] = df.loc[df[col].notna(), col].str.replace(".0", "", regex=False)

    return df


def read_diplome(source, an):
    os.chdir(DATA_PATH)
    # logger.debug(f'reading parquet file diplomes for {source} {an} ...')
    sortie = str(an)[2:4]
    file = get_filename(source, sortie)
    df = pd.read_parquet(f'{DATA_PATH}{file}')

    if source in ['culture', 'ens', 'priv', 'result']:
        clef = source + sortie
    else:
        clef = 'culture' + sortie

    df = df.astype(typesd.types[clef])
    df = annee(df)

    df.loc[:,'SOURCE'] = source
    df.loc[:,'ANNEE'] = str(an)

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.split('.0', regex=False).str[0].str.strip()
            df[col] = df[col].str.replace('nan', '', regex=False)
            df[col] = df[col].str.replace(u'\xa0', ' ').str.strip()
        if col != col.upper():
            df.rename(columns={col: col.upper()}, inplace=True)
        
    # logger.debug(f'merging with FORMAT rattach')
    # df = pd.merge(df, df_rattach, on='COMPOS', how='left')
    # logger.debug('done')
    return df

ens21 = read_diplome("ens", 2021)

