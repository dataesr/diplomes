import os
import pandas as pd
import numpy as np
from diplomes import dtypes_inputs as typesd
from diplomes.u1_google_sheets import get_all_correctifs
import requests
from retry import retry

from application.server.main.logger import get_logger

logger = get_logger(__name__)

pd.options.mode.chained_assignment = None

# DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")
DATA_PATH = "/run/media/julia/DATA/diplomes_donnees/"


def get_filename(source, an):
    if source == "result":
        filename = f'corrected_{source}{an}_ssa.parquet'
    elif source == "enq":
        filename = f'corrected_dip{source}26bis{an}_ssa.parquet'
    else:
        filename = f'corrected_dip{source}{an}_ssa.parquet'
    return filename


def read_diplome(source, an):
    os.chdir(DATA_PATH)
    logger.debug(f'reading parquet file diplomes for {source} {an} ...')
    sortie = str(an)
    sort = str(an)[2:4]
    file = get_filename(source, sortie)
    df = pd.read_parquet(f'{DATA_PATH}parquet/{file}')

    if source in ['ens', 'priv', 'result']:
        clef = 'cor_' + source + sort
    else:
        clef = 'cor_culture' + sort

    df = df.astype(typesd.types[clef])

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.split('.0', regex=False).str[0].str.strip()
            df[col] = df[col].str.replace('nan', '', regex=False)
            df[col] = df[col].str.replace(u'\xa0', ' ').str.strip()
        if col != col.upper():
            df.rename(columns={col: col.upper()}, inplace=True)

    for col in ["TYP_DIPL", "TYP_DIPR", "TYP_DIPINT"]:
        if col in df.columns:
            df[col] = df[col].fillna("")
            df.loc[df[col].isin(["None", None]), col] = ""
            df[col] = df[col].apply(lambda x: "0" + x if x.isdigit() and len(x) == 1 else x)

    df = df.drop_duplicates().reset_index(drop=True)
    return df


def requete(ul: str, form: str, ky: str) -> object:
    """
    This function queries a website.
    """
    adr = ul + form + ky
    res = requests.get(adr)
    status = res.status_code
    if status != 200:
        raise ConnectionError(f"Failed while trying to access the URL with status code {status}")
    else:
        print("URL successfully accessed", flush=True)
    return res


@retry(tries=3, delay=5, backoff=5)
def df_req(ul: str, frm: str, ky: str) -> pd.DataFrame:
    """
    This functions takes the text result (CSV with ; as separator) from the query, writes it and reads it as a df.
    It returns a df
    """
    res = requete(ul, frm, ky)
    text = res.text
    with open("structures.csv", "w") as f:
        f.write(text)
    df = pd.read_csv("structures.csv", sep=";", encoding="utf-8", engine="python")
    return df


culture21 = read_diplome("culture", 2020)
col_c21 = list(culture21.columns)
col_c21.sort()
culture21 = culture21[col_c21]
paysage_culture = ["RESDIP", "RESINT", "TYP_DIPL", "NIVEAU", "CURSUS_LMDR"]
for col in col_c21:
    if "PAYSAGE" in col:
        paysage_culture.append(col)
paysage_culture.sort()
culture_dip = culture21[paysage_culture].drop_duplicates().reset_index(drop=True)

enq21 = read_diplome("enq", 2020)
col_enq21 = list(enq21.columns)
col_enq21.sort()
enq21 = enq21[col_enq21]
paysage_enq = ["RESDIP", "RESINT", "TYP_DIPL", "NIVEAU", "CURSUS_LMDR"]
for col in col_enq21:
    if "PAYSAGE" in col:
        paysage_enq.append(col)
paysage_enq.sort()
enq_dip = enq21[paysage_enq].drop_duplicates().reset_index(drop=True)

ens21 = read_diplome("ens", 2020)
col_ens21 = list(ens21.columns)
col_ens21.sort()
ens21 = ens21[col_ens21]
paysage_ens = ["RESDIP", "RESINT", "TYP_DIPL", "TYP_DIPINT", "NIVEAU", "CURSUS_LMDR", "CURSUS_LMD_INT"]
for col in col_ens21:
    if "PAYSAGE" in col:
        paysage_ens.append(col)
paysage_ens.sort()
ens_dip = ens21[paysage_ens].drop_duplicates().reset_index(drop=True)

inge21 = read_diplome("inge", 2020)
col_inge21 = list(inge21.columns)
col_inge21.sort()
inge21 = inge21[col_inge21]
paysage_inge = ["RESDIP", "RESINT", "TYP_DIPL", "TYP_DIPINT", "NIVEAU", "CURSUS_LMDR", "CURSUS_LMD_INT"]
for col in col_inge21:
    if "PAYSAGE" in col:
        paysage_inge.append(col)
paysage_inge.sort()
inge_dip = inge21[paysage_inge].drop_duplicates().reset_index(drop=True)

mana21 = read_diplome("mana", 2020)
col_mana21 = list(mana21.columns)
col_mana21.sort()
mana21 = mana21[col_mana21]
paysage_mana = ["RESDIP", "RESINT", "TYP_DIPL", "NIVEAU", "CURSUS_LMDR"]
for col in col_mana21:
    if "PAYSAGE" in col:
        paysage_mana.append(col)
paysage_mana.sort()
mana_dip = mana21[paysage_mana].drop_duplicates().reset_index(drop=True)

priv21 = read_diplome("priv", 2020)
col_priv21 = list(priv21.columns)
col_priv21.sort()
priv21 = priv21[col_priv21]
paysage_priv = ["RESDIP", "RESINT", "TYP_DIPL", "TYP_DIPINT", "NIVEAU", "CURSUS_LMDR", "CURSUS_LMD_INT"]
for col in col_priv21:
    if "PAYSAGE" in col:
        paysage_priv.append(col)
paysage_priv.sort()
priv_dip = priv21[paysage_priv].drop_duplicates().reset_index(drop=True)

result21 = read_diplome("result", 2020)
col_result21 = list(result21.columns)
col_result21.sort()
result21 = result21[col_result21]
paysage_result = ["RESDIP", "RESINT", "TYP_DIPL", "TYP_DIPINT", "NIVEAU", "CURSUS_LMDR", "CURSUS_LMD_INT"]
for col in col_result21:
    if "PAYSAGE" in col:
        paysage_result.append(col)
paysage_result.sort()
result_dip = result21[paysage_result].drop_duplicates().reset_index(drop=True)
# col_od = ["ANNEE_UNIVERSITAIRE", "SESSION", "SEXE", "BAC_RGRP", "BAC", "AGE_BAC", ""]
# result_od = result21[]

cor_dict = get_all_correctifs("json")

c_etab = pd.DataFrame(cor_dict['C_ETABLISSEMENTS'])
d_epe = pd.DataFrame(cor_dict['D_EPE'])
e_form_ens = pd.DataFrame(cor_dict['E_FORM_ENS'])
k_form_ens_etab = pd.DataFrame(cor_dict['K_FORM_ENS_ETAB'])
communes = pd.DataFrame(cor_dict['LES_COMMUNES'])

col_cetab = ["id_paysage", "id_paysage_actuel", "uo_lib", "type", "typologie_d_universites_et_assimiles",
             "anciens_codes_uai",
             "identifiant_wikidata", "identifiant_ror", "com_code"]
col_cetab = [x.upper() for x in col_cetab]
col_cetab.sort()

c_etab2 = c_etab[col_cetab]

url = "https://data.enseignementsup-recherche.gouv.fr/explore/dataset/fr_esr_sise_diplomes_delivres_esr_public/download/"

form = "?format=csv&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"

key = f"&apikey=7240d81b7c8f9c5a13937cf8a08150c7096ede2f0738aebe80e65104"

diplomes = df_req(url, form, key)

# communes2 = communes[["COM_CODE", "COM_NOM", "DEP_ID", "DEP_NOM", "ACA_ID", "ACA_NOM", "REG_ID",
#                       "REG_NOM"]].drop_duplicates().reset_index(drop=True)
#
# communes2["combinaison"] = communes2.apply(
#     lambda a: a["REG_NOM"] + ">" +
#               a["DEP_NOM"] + ">" +
#               a["COM_NOM"] if "Paris " in a["COM_NOM"] else a["REG_NOM"] + ">" +
#                                                             a["DEP_NOM"] + ">" +
#                                                             a["ACA_NOM"] + ">" +a["COM_NOM"],
#     axis=1)


