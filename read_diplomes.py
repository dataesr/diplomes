import pandas as pd
import os
from logger import get_logger
import dtype_diplomes as types

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")


def get_filename(source, rentree):
    filename = f'{source}_{rentree}'
    return filename


def get_sources(annee):
    assert (annee >= 2004)
    sources = ['inge', 'priv']
    if annee > 2004:
        sources.append('ens')
    if annee > 2005:
        sources.append('mana')
    if annee > 2015:
        sources.append('enq26bis')
    if annee > 2016:
        sources.append('culture')
    return sources


def read_parquet(source, annee):
    os.chdir(DATA_PATH)
    logger.debug(f'reading sas file sise for {source} {annee} ...')
    df = pd.read_parquet(f"{DATA_PATH}dip{source}{str(annee)[2:4]}_ssa.parquet")

    df.loc[:,'SOURCE'] = source
    df.loc[:,'ANNEE'] = annee

    if source in ['culture', 'enq', 'inge', 'mana']:
        clef = "culture21"
    else:
        clef = source + str(annee)[2:4]

    if clef in types.keys():
        for cle in types[clef].keys():
            if cle in list(df.columns):
                df[clef] = df.loc[:,cle].astype(types[clef][cle])
    else:
        for col in list(df.columns):
            if df[col].dtype == float:
                df[col] = df[col].astype(pd.Int64Dtype())

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.split('.0', regex=False).str[0].str.strip()
            df[col] = df[col].str.replace('nan', '', regex=False)
            df[col] = df[col].str.replace(u'\xa0', ' ').str.strip()
    # logger.debug(f'merging with FORMAT rattach')
    # df = pd.merge(df, df_rattach, on='COMPOS', how='left')
    for c in df.columns:
        if c != c.upper():
            df.rename(columns={c: c.upper()}, inplace=True)
    logger.debug('done')
    return df


def read_rattach(rentree):
    os.chdir(DATA_PATH)
    logger.debug(f'read RATTACH for year {rentree} ...')
    df_format, meta_format = pyreadstat.read_sas7bcat(f'{DATA_PATH}FORMAT/inscri{str(rentree)[2:4]}/formats.sas7bcat',
                                                      encoding='iso-8859-1')
    data_rattach = []
    for compos in meta_format.value_labels['$RATTACH']:
        rattach = meta_format.value_labels['$RATTACH'][compos]
        data_rattach.append({'COMPOS': compos, 'RATTACH': rattach})
    df_rattach = pd.DataFrame(data_rattach)
    logger.debug('done')
    return df_rattach


def read_cometab(rentree):
    os.chdir(DATA_PATH)
    logger.debug(f'read COMETAB for year {rentree} ...')
    df_format, meta_format = pyreadstat.read_sas7bcat(f'{DATA_PATH}FORMAT/inscri{str(rentree)[2:4]}/formats.sas7bcat',
                                                      encoding='iso-8859-1')

    data_COMCOMP = []
    for start in meta_format.value_labels['$COMCOMP']:
        COMCOMP = meta_format.value_labels['$COMCOMP'][start]
        data_COMCOMP.append({'UAI': start, 'COMCODE': COMCOMP})
    df_cometab = pd.DataFrame(data_COMCOMP)
    logger.debug('done')
    return df_cometab


def to_parquet(df, filename):
    os.chdir(DATA_PATH)
    logger.debug(f'saving parquet file sise for {filename} ...')
    os.system(f'mkdir -p {DATA_PATH}parquet')
    df.to_parquet(f'{DATA_PATH}parquet/{filename}.parquet')
    logger.debug('done')
