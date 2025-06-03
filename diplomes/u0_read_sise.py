import numpy as np
import pandas as pd
import os
import random
import string
import re
from application.server.main.logger import get_logger
from diplomes import dtypes_inputs as typesd
import pyreadstat

random.seed(42)

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")


def get_sources(annee):
    assert (annee >= 2015)
    sources = ['result', 'ens', 'inge', 'priv']
    if annee > 2017:
        sources.append('mana')
    if annee > 2018:
        sources.append('enq')
        sources.append('culture')
    return sources


def list_id(lng: int, nb: int) -> list:
    """
    Don't keep E/e and zero as first character.
    Generate codes of a certain size defined by the user.

    """
    letters = string.hexdigits
    letters = letters.replace("e", "")
    letters = letters.replace("E", "")
    lng_let = len(letters)
    liste = []
    for n in range(nb * 2):
        res = "".join(random.choice(letters) for i in range(lng))
        liste.append(res)

    liste = list(set(liste))
    liste = [x for x in liste if not re.search(r"^0.+", x)][0:nb]

    return liste


def get_filename(source, an):
    if source == "result":
        filename = f'{source}{an}_ssa.parquet'
    elif source == "enq":
        filename = f'dip{source}26bis{an}_ssa.parquet'
    else:
        filename = f'dip{source}{an}_ssa.parquet'
    return filename


def get_filename_sas(source, an):
    if int(an) >= 15:
        if source == "result":
            filename = f'{source}{an}_ssa.sas7bdat'
        elif source == "enq":
            filename = f'dip{source}26bis{an}_ssa.sas7bdat'
        else:
            filename = f'dip{source}{an}_ssa.sas7bdat'
    return filename


def annee(df: pd.DataFrame):
    col_an = ["ANBAC", "ANNAIS", "ANINSC"]
    for col in col_an:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    return df


def read_sise_sas():
    for an in range(2015, 2021):
        sources = get_sources(an)
        sortie = str(an)[2:4]
        for sour in sources:
            if sour == "result":
                name = f"result{sortie}_ssa.parquet"
            elif sour == "enq":
                name = f"enq26bis{sortie}_ssa.parquet"
            else:
                name = f"dip{sour}{sortie}_ssa.parquet"
            if name not in os.listdir(f"{DATA_PATH}parquet/"):
                clef = sour + sortie

                filename = get_filename_sas(sour, sortie)
                logger.debug(f'reading sas file sise for {sour} {an} ...')
                df, _ = pyreadstat.read_sas7bdat(f'{DATA_PATH}sas/inputs/{filename}',
                                                 encoding='iso-8859-1')
                col_df = list(df.columns)
                col_df.sort()
                dico = {}
                for col in col_df:
                    dico[col] = col.upper()
                df = df.rename(columns=dico)
                col_df = [col.upper() for col in col_df]
                df = df[col_df]

                for col in list(df.columns):
                    if df[col].dtype == float:
                        df[col] = df[col].astype(pd.Int64Dtype())

                for col in df.columns:
                    if df[col].dtype == object:
                        df[col] = df[col].str.split('.0', regex=False).str[0].str.strip()
                        df[col] = df[col].str.replace('nan', '', regex=False)
                        df[col] = df[col].str.replace(u'\xa0', ' ').str.strip()

                logger.debug(f'saving parquet file sise for {name} ...')
                df.to_parquet(f'{DATA_PATH}parquet/{name}')
    logger.debug('done')


def read_diplome(source, an):
    os.chdir(DATA_PATH)
    read_sise_sas()
    sortie = str(an)[2:4]
    file = get_filename(source, sortie)
    if file in os.listdir(f'{DATA_PATH}parquet/'):
        df = pd.read_parquet(f'{DATA_PATH}parquet/{file}')
    else:
        print("file not found")

    clef = source + sortie
    print(clef)

    if "RESDIP" not in df.columns:
        df["RESDIP"] = "O"

    if "RESINT" not in df.columns:
        df["RESINT"] = "N"

    if "FLAG_DIP" in df.columns:
        df = df.loc[df['FLAG_DIP'] != "1"]

    if "FLAG_RES" in df.columns:
        df = df.loc[df['FLAG_RES'] != "1"]

    if "ETATINS" in df.columns:
        df = df.loc[df["ETATINS"] != "41"]

    if source == "priv":
        df.loc[(df["RESINT"] == "O") & (df["RESDIP"] == "N") & (df["TYP_DIPR"] == "XA") & (df["NIVEAUR"] == "02") & (
            df["TYP_DIPINT"].isin(["", "None", np.nan, None])), "TYP_DIPINT"] = "CP"
        df.loc[(df["RESINT"] == "O") & (df["RESDIP"] == "N") & (df["TYP_DIPR"] == "XB") & (df["NIVEAUR"] == "02") & (
            df["TYP_DIPINT"].isin(["", "None", np.nan, None])), "TYP_DIPINT"] = "EP"

    if source == "result":
        df.loc[(df["DIPINT"] == "5000770") & (df["SECTINT"] == "07"), "DISCIPLINT"] = "11"

    if "TYP_DIPINT" in df.columns and "RESINT" in df.columns:
        df.loc[(df["RESINT"].isin([np.nan, None, "", "None"])) & (
            df["TYP_DIPINT"].isin(["", "None", np.nan, None])), "RESINT"] = "N"

    if source == "inge":
        df["FLAG_MEEF"] = "0"
        if "RESINT" in df.columns:
            df.loc[df["RESINT"] != "O", "RESDIP"] = "O"
        else:
            df["RESDIP"] = "O"
    if source == "ens":
        df["FLAG_MEEF"] = "0"
        df.loc[~df["DIPINT"].isin([np.nan, "", None, "None"]), "RESINT"] = "O"
        df.loc[~df["DIPINT"].isin([np.nan, "", None, "None"]), "RESDIP"] = "N"
        df.loc[df["DIPINT"].isin([np.nan, "", None, "None"]), "RESINT"] = "N"
        df.loc[df["DIPINT"].isin([np.nan, "", None, "None"]), "RESDIP"] = "O"
    if source in ["mana", "enq", "culture"]:
        df["FLAG_MEEF"] = "0"
        df["RESDIP"] = "O"
        df["RESINT"] = ""

    if "SECTDISR" in df.columns:
        df.loc[df["SECTDISR"].isin(["2A", "2B", "2C", "2E"]), "DISCIPLIR"] = "04"
        df.loc[df["SECTDISR"].isin(["2A", "2B", "2C", "2E"]), "SECTDISR"] = "21"

        df.loc[df["SECTDISR"] == "2D", "DISCIPLIR"] = "04"
        df.loc[df["SECTDISR"] == "2D", "SECTDISR"] = "27"

    elif "SECTDIS" in df.columns:
        df.loc[df["SECTDIS"].isin(["2A", "2B", "2C", "2E"]), "DISCIPLIR"] = "04"
        df.loc[df["SECTDIS"].isin(["2A", "2B", "2C", "2E"]), "SECTDIS"] = "21"

        df.loc[df["SECTDIS"] == "2D", "DISCIPLIR"] = "04"
        df.loc[df["SECTDIS"] == "2D", "SECTDIS"] = "27"

    df = annee(df)

    d_types = typesd.types[clef]
    if "FLAG_MEEF" in df.columns:
        if "FLAG_MEEF" in d_types.keys():
            del d_types["FLAG_MEEF"]

    df = df.astype(d_types)
    if "FLAG_MEEF" in df.columns:
        if df["FLAG_MEEF"].dtype != pd.Int64Dtype():
            df["FLAG_MEEF"] = df["FLAG_MEEF"].astype(str)
            df["FLAG_MEEF"] = df["FLAG_MEEF"].str.split('.0', regex=False).str[0].str.strip()

    if "IDENTITFIANT_NATIONAL_ETUDIANT" in df.columns:
        df = df.rename(columns={"IDENTITFIANT_NATIONAL_ETUDIANT": "IDETU"})
    if "IDETU_CORR" in df.columns:
        df = df.rename(columns={"IDETU_CORR": "IDETU1"})

    if "NUMED" in df.columns:
        df.loc[df["NUMED"].isin([np.nan, None, "", "None"]), "NUMED"] = ""
        df.loc[df["NUMED"] == "050", "NUMED"] = "50"

    if "DIPDER" in df.columns:
        df.loc[df["DIPDER"].isin(["", "None", np.nan, None]), "DIPDER"] = "$"

    if "VOIER" in df.columns:
        df.loc[df["VOIER"].isin(["", "None", np.nan, None]), "VOIER"] = "9"

    if "PAYS_MOBCO" in df.columns:
        df.loc[df["PAYS_MOBCO"] == "", "PAYS_MOBCO"] = "000"

    if "TYPE_MOBCO" in df.columns:
        df.loc[df["TYPE_MOBCO"].isin(["", "0"]), "TYPE_MOBCO"] = "X"

    if "NATION" in df.columns:
        df.loc[df["NATION"].isin(["", "None", np.nan, None]), "NATION"] = "$"
        df.loc[df["NATION"].isin(["20", "22", "86"]), "NATION"] = "999"

    if "BAC" in df.columns:
        df.loc[df["BAC"].isin(["", "None", np.nan, None]), "BAC"] = "9999"
        df.loc[df["BAC"].isin(["A2-8", "A6", "A6-8", "A7", "A7-8"]), "BAC"] = df.loc[
            df["BAC"].isin(["A2-8", "A6", "A6-8", "A7", "A7-8"]), "BAC"].apply(
            lambda a: a + "5" if "-" in a else a + "-85")

    if "PCSPAR" in df.columns:
        df.loc[df["PCSPAR"].isin(["", "None", np.nan, None]), "BAC"] = "99"

    if "BAC_RGRP" in df.columns:
        df.loc[df["BAC_RGRP"].isin(["", "None", np.nan, None]), "BAC_RGRP"] = "9"

    if "FR_ETR" in df.columns:
        df.loc[df["FR_ETR"].isin(["", "None", np.nan, None]), "FR_ETR"] = "9"

    if "SEXE" in df.columns:
        df.loc[df["SEXE"].isin(["", "None", np.nan, None]), "SEXE"] = "9"

    if "TYP_DIPINT" in df.columns:
        df.loc[(df["RESDIP"] == "O") & (df["TYP_DIPINT"] == "10"), "TYP_DIPINT"] = "CP"
        df.loc[(df["RESDIP"] == "O") & (df["TYP_DIPINT"] == "20"), "TYP_DIPINT"] = "EP"
        df.loc[(df["RESDIP"] == "O") & (df["TYP_DIPINT"].isin([np.nan, "", None, "None"])) & (
                    df["TYP_DIPL"] == "PE"), "TYP_DIPINT"] = "CZ"
        df.loc[(df["RESINT"] == "O") & (df["TYP_DIPR"] == "XA") & (df["TYP_DIPINT"].isin(["01", "10", "CC", "XA"])) & (
                df["NIVEAUR"] == "02"), "TYP_DIPINT"] = "CP"

    df.loc[:, 'SOURCE'] = source
    df.loc[:, 'RENTREE'] = str(an)
    df.loc[:, 'ANNEE_UNIVERSITAIRE'] = str(an) + "-" + str(an + 1)[2:4]
    df.loc[:, 'SESSION'] = str(an + 1)

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.split('.0', regex=False).str[0].str.strip()
            df.loc[df[col].isin(["nan", "None", None]), col] = np.nan
            df[col] = df[col].str.replace(u'\xa0', ' ').str.strip()
        if col != col.upper():
            df.rename(columns={col: col.upper()}, inplace=True)

    for col in ["TYP_DIPL", "TYP_DIPR", "TYP_DIPINT"]:
        if col in df.columns:
            df[col] = df[col].fillna("")
            df.loc[df[col].isin(["None", None, np.nan]), col] = ""
            df[col] = df[col].apply(lambda x: "0" + x if x.isdigit() and len(x) == 1 else x)

    if "TYP_DIPINT" in df.columns and "RESINT" in df.columns:
        df.loc[(df["RESINT"] != "N") & (df["TYP_DIPINT"].isin(["", np.nan, None, "None"])), "RESINT"] = "N"
        df.loc[(df["RESINT"] == "O") & (df["TYP_DIPR"] == "XA") & (df["TYP_DIPINT"].isin(["01", "10", "CC", "XA"])) & (
                df["NIVEAUR"] == "02"), "TYP_DIPINT"] = "CP"
        df.loc[(df["RESDIP"] == "N") & (df["RESINT"] == "O") & (df["DIPINT"] != "") & (
                    df["TYP_DIPINT"] == "10"), "TYP_DIPINT"] = "CP"
        df.loc[(df["RESDIP"] == "N") & (df["RESINT"] == "O") & (df["DIPINT"] != "") & (
                df["TYP_DIPINT"] == "20"), "TYP_DIPINT"] = "EP"
        df.loc[(df["RESDIP"] == "N") & (df["RESINT"] == "O") & (df["TYP_DIPINT"].isin([np.nan, "", None, "None"])) & (
                df["TYP_DIPINT"] == "5000770") & (df["TYP_DIPR"] == "PE"), "SECTINT"] = "07"
        df.loc[(df["RESDIP"] == "N") & (df["RESINT"] == "O") & (df["TYP_DIPINT"].isin([np.nan, "", None, "None"])) & (
                df["TYP_DIPINT"] == "5000770") & (df["TYP_DIPR"] == "PE"), "TYP_DIPINT"] = "CZ"
        df.loc[(df["TYP_DIPINT"] == "CC") & (df["TYP_DIPR"] == "XA") & (df["RESINT"] == "O"), "TYP_DIPINT"] = "CP"

    if source in ["result", "priv"]:
        df = df.loc[(df["RESDIP"] == "O") | (df["RESINT"] == "O")]
    logger.debug('done')
    return df


def to_parquet(df, filename):
    os.chdir(DATA_PATH)
    logger.debug(f'saving parquet file sise for {filename} ...')
    os.system(f'mkdir -p {DATA_PATH}parquet')
    df.to_parquet(f'{DATA_PATH}parquet/{filename}.parquet')
    logger.debug('done')


def read_rattach(an):
    os.chdir(DATA_PATH)
    logger.debug(f'read RATTACH for year {an} ...')
    fichiers = os.listdir(DATA_PATH + "parquet")
    if f'rattach{str(an)[2:4]}.parquet' not in fichiers:
        df_format, meta_format = pyreadstat.read_sas7bcat(
            f'{DATA_PATH}FORMAT/inscri{str(an)[2:4]}/formats.sas7bcat',
            encoding='iso-8859-1')
        data_rattach = []
        for compos in meta_format.value_labels['$RATTACH']:
            rattach = meta_format.value_labels['$RATTACH'][compos]
            data_rattach.append({'COMPOS': compos, 'RATTACH': rattach})
        df_rattach = pd.DataFrame(data_rattach)
        to_parquet(df_rattach, f'rattach{str(an)[2:4]}')
    else:
        df_rattach = pd.read_parquet(f'{DATA_PATH}parquet/rattach{str(an)[2:4]}.parquet')
    logger.debug('done')

    return df_rattach


def read_cometab(rentree):
    os.chdir(DATA_PATH)
    logger.debug(f'read COMETAB for year {rentree} ...')
    fichiers = os.listdir(DATA_PATH + "parquet")
    if f'cometab{str(rentree)[2:4]}.parquet' not in fichiers:
        df_format, meta_format = pyreadstat.read_sas7bcat(
            f'{DATA_PATH}FORMAT/inscri{str(rentree)[2:4]}/formats.sas7bcat',
            encoding='iso-8859-1')

        data_COMCOMP = []
        for start in meta_format.value_labels['$COMCOMP']:
            COMCOMP = meta_format.value_labels['$COMCOMP'][start]
            data_COMCOMP.append({'UAI': start, 'COMCODE': COMCOMP})
        df_cometab = pd.DataFrame(data_COMCOMP)
        to_parquet(df_cometab, f'cometab{str(rentree)[2:4]}')
    else:
        df_cometab = pd.read_parquet(f'{DATA_PATH}parquet/cometab{str(rentree)[2:4]}.parquet')
    logger.debug('done')
    return df_cometab
