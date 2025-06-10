import time

import pandas as pd
import numpy as np

from diplomes.u0_read_sise import *
from diplomes.u1_google_sheets import get_all_correctifs
from diplomes.u2_cleaning_functions import *
from application.server.main.logger import get_logger
from utils import swift

logger = get_logger(__name__)

pd.options.mode.chained_assignment = None


def corrige_df(df, source, rentree, cor_dict):
    start_main = time.time()

    df["AGE_BAC"] = df["ANBAC"] - df["ANNAIS"]

    if "RESDIP" in df.columns:
        df = effectif_resdip(df)
    if "RESINT" in df.columns:
        df = effectif_resint(df)
        if "TYP_DIPINT" and "ETABLI_ORI_UAI" in df.columns:
            df.loc[(df["TYP_DIPINT"] == "UT") & (df["RESDIP"] == "X") & (df["RESINT"] == "O") & (
                    df["ETABLI_ORI_UAI"] == "0912423P") & (df["TYP_DIPL"] == "UT"), ["RESDIP", "RESINT", "EFFECTIF_R",
                                                                                     "EFFECTIF_INT"]] = ["O", "X", 1, 0]

    print("COMPOS", flush=True)
    df = fill_COMPOS(df)

    print("NUMED", flush=True)
    df = corrige_NUMED(df)

    df.loc[df["ETABLI"] == "0352771X", "ETABLI"] = "0352692L"

    df['ETABLI_ORI_UAI'] = df['ETABLI']

    add_cols = ['ETABLI_DIFFUSION', 'ID_PAYSAGE_FORMENS', 'COMUI', 'DEPBAC', 'ACABAC', 'DEPRESPA', 'CONV', 'PAR_TYPE',
                'ETABLIESPE']
    for col in add_cols:
        if col not in df.columns:
            df.loc[:, col] = ''

    #############################################
    end = time.time()
    print(f"fin fill_compos/numed -> {end - start_main}", flush=True)
    start = time.time()
    ################################################
    print("delete", flush=True)
    df = delete(df, cor_dict)

    df['UI'] = df['COMPOS']
    df.loc[df["UI"].isin(
        ["0753364Z", "0352317D", "0595876S", "0380134P", "0130221V", "0310133B", "0330192E", "0690173N"]), "ETABLI"] = \
        df.loc[df["UI"].isin(
            ["0753364Z", "0352317D", "0595876S", "0380134P", "0130221V", "0310133B", "0330192E", "0690173N"]), "UI"]
    df.loc[df["UI"].isin(
        ["0753364Z", "0352317D", "0595876S", "0380134P", "0130221V", "0310133B", "0330192E",
         "0690173N"]), "ETABLI_ORI_UAI"] = \
        df.loc[df["UI"].isin(
            ["0753364Z", "0352317D", "0595876S", "0380134P", "0130221V", "0310133B", "0330192E", "0690173N"]), "ETABLI"]

    ########################################
    end = time.time()
    print(f"fin delete -> {end - start}", flush=True)
    start = time.time()
    #######################################

    if source == "priv":
        mask_priv = ((df['ID_PAYSAGE_FORMENS'] == '') | (pd.isna(df['ID_PAYSAGE_FORMENS'])) | (
            df['ID_PAYSAGE_FORMENS'].isnull())) & (df['SOURCE'] == 'priv')
        if "FLAG_MEEF" in df.columns:
            df["FLAG_MEEF"] = df["FLAG_MEEF"].astype(str)
        df.loc[mask_priv, 'FLAG_MEEF'] = "1"
        df.loc[mask_priv, 'EFF_SS_ESPE'] = 0

    df.loc[(df.RENTREE == 2010) & (df.COMPOS == "9830491S"), 'UI'] = "9830445S"
    df.loc[df.COMPOS == "0684045X", 'UI'] = "0694045X"

    print("read rattach", flush=True)
    rattach = read_rattach(rentree)
    df = df.merge(rattach, how='left', on='COMPOS')
    df.loc[(df['RATTACH'] == '') | (df['RATTACH'].isnull()), "RATTACH"] = df.loc[
        (df['RATTACH'] == '') | (df['RATTACH'].isnull()), "COMPOS"]

    df.loc[df.COMPOS == "0011312W", 'UR'] = "0694121E"
    df.loc[df.COMPOS == "0133479L", 'UR'] = "0131844J"

    ########################################
    end = time.time()
    print(f"fin rattach -> {end - start}", flush=True)
    start = time.time()

    print("etabli", flush=True)
    df = corrige_ETABLI(df, cor_dict)

    print("enrich_a_uai", flush=True)
    df = enrich_a_uai(df, cor_dict)

    print("enrich_d_epe", flush=True)
    df = enrich_d_epe(df, cor_dict)

    print("etabli diffusion", flush=True)
    df = corrige_ETABLI_DIFFUSION(df, cor_dict)

    # ###########################################
    end = time.time()
    print(f"fin etabli/comins -> {end - start}", flush=True)
    start = time.time()
    # ############################################

    print("cometab", flush=True)
    cometab_form = read_cometab(rentree)

    # COMINS
    df = df.merge(cometab_form, how='left', left_on='COMPOS', right_on='UAI').rename(
        columns={'COMCODE': 'COMINS'}).reset_index(drop=True).drop(columns="UAI")

    print("comins", flush=True)
    df = corrige_COMINS(df, cor_dict)

    # COMETAB
    df = df.merge(cometab_form, how='left', left_on='ETABLI', right_on='UAI').reset_index(drop=True)

    df.loc[(df['ETABLI'].isin(['9730224F', '9730279R', '9741101D'])) | (
            df['COMCODE'] != df['ETABLI'].str[:5]), 'COMETAB'] = df.loc[
        (df['ETABLI'].isin(['9730224F', '9730279R', '9741101D'])) | (df['COMCODE'] != df['ETABLI'].str[:5]), 'COMCODE']

    df = corrige_COMETAB(df, cor_dict)

    # #####################################
    end = time.time()
    print(f"fin cometab -> {end - start}", flush=True)
    start = time.time()
    # #####################################

    # COMUI
    df.loc[df.COMINS != '', 'COMUI'] = df.loc[df.COMINS != '', 'COMINS']
    df.loc[(df['COMINS'] == df['COMPOS'].str[:5]) & (
        ~df['UI'].isin(['9730224F', '9730279R', '9741101D', '9730269E', '9741183T', '9730224F'])), 'COMUI'] = ''

    df = corrige_COMUI(df, cor_dict)

    #################################
    end = time.time()
    print(f"fin comui -> {end - start}", flush=True)
    start = time.time()
    # ###################################

    mask_sitrupe = (df.SITUPRE == '') | (df.SITUPRE.isnull()) | (pd.isna(df.SITUPRE))
    df.loc[mask_sitrupe, 'SITUPRE'] = '9'

    df.loc[(df.BAC.isin(['0037', '0031'])) & (df.BAC_RGRP != '7'), 'BAC_RGRP'] = '7'
    df.loc[(df.BAC.isin(['ES'])) & (df.BAC_RGRP != '2'), 'BAC_RGRP'] = '2'

    if 'NATION' in df.columns:
        df.loc[df['NATION'].isin(["600", "900", "993", "996", "998", "$"]), 'NATION'] = "999"
    else:
        df['NATION'] = ''

    mask_rentree = (df.RENTREE.astype(int) > 2003) & (df.RENTREE.astype(int) < 2009)
    df.loc[mask_rentree, 'PCSPAR2'] = "99"

    print("ca/dep/respa...", flush=True)
    df = corr_dep_aca_respa("DEP_ACA_RESPA_CORRECTIF", df, cor_dict)

    df = corr_reste_groupe("RESTE_DEPRESPA_CORRECTIF", df, cor_dict)

    # ##################################
    end = time.time()
    print(f"fin aca/dep/respa... -> {end - start}", flush=True)
    start = time.time()
    # #################################

    print("aca/dep correctifs", flush=True)
    df = corr_dep_aca("DEP_CORRECTIF", 'DEPBAC', df, cor_dict)

    if any(df.DEPBAC != '000'):
        df = corr_dep_aca("ACA_CORRECTIF", 'ACABAC', df, cor_dict)

    df = corr_dep_aca("DEP_CORRECTIF", 'DEPRESPA', df, cor_dict)

    if any(df.DEPRESPA != '000'):
        df = corr_dep_aca("ACA_CORRECTIF", 'ACARESPA', df, cor_dict)

    # #################################
    end = time.time()
    print(f"fin aca/dep correctifs -> {end - start}", flush=True)
    start = time.time()
    # #################################

    print("formations", flush=True)
    df = corrige_FORMATIONS(df, cor_dict)

    ###################################
    end = time.time()
    print(f"fin formations -> {end - start}", flush=True)
    start = time.time()
    #####################################

    # # SI NIVEAU et SECTDIS = 1 caractere numerique alors ajouter un '0' devant*/
    if 'TYP_DIPINT' in df.columns:
        cols = ['NIVEAU', 'SECTDIS', 'DISCIPLI', 'TYP_DIPL', 'TYP_DIPINT']
    else:
        cols = ['NIVEAU', 'SECTDIS', 'DISCIPLI', 'TYP_DIPL']
    for c in cols:
        mask = (df[c].str.len() == 1) & (df[c].str.isnumeric())
        df.loc[mask, c] = df.loc[mask, c].str.rjust(2, fillchar='0')

    df.loc[df.SECTDIS.isin(['44', '45', '46', '47', '48']), 'SECTDIS'] = "39"

    print("cursusLMD/groupe", flush=True)
    df = corrige_cursus_lmd(df, cor_dict)

    if source == "priv":
        df.loc[
            (df["CURSUS_LMD_INT"] == "X") & (df["RESINT"] == "O") & (df["TYP_DIPINT"] == "CP"), "CURSUS_LMD_INT"] = "L"
        df.loc[
            (df["CURSUS_LMD_INT"] == "X") & (df["RESINT"] == "O") & (df["TYP_DIPINT"] == "EP"), "CURSUS_LMD_INT"] = "M"
        df.loc[
            (df["CURSUS_LMD_INT"] == "X") & (df["RESINT"] == "O") & (df["TYP_DIPINT"] == "") & (
                    df["TYP_DIPL"] == "XB"), "CURSUS_LMD_INT"] = "M"
        df.loc[
            (df["CURSUS_LMD_INT"] == "X") & (df["RESINT"] == "O") & (df["TYP_DIPINT"] == "") & (
                    df["TYP_DIPL"] == "XB"), "TYP_DIPINT"] = "EP"
        df.loc[
            (df["CURSUS_LMD_INT"] == "X") & (df["RESINT"] == "O") & (df["TYP_DIPINT"] == "") & (
                df["TYP_DIPL"].isin(["TB", "TE"])), "CURSUS_LMD_INT"] = "D"

    df = corr_reste_groupe("GROUPE_CORRECTIF", df, cor_dict)

    df.loc[(df.REGIME == '') | (df.REGIME.isnull()), 'REGIME'] = '99'

    ###############################
    end = time.time()
    print(f"fin cursusLMD/groupe -> {end - start}", flush=True)
    start = time.time()
    ###############################

    print("enrich", flush=True)
    df = enrich_les_communes(df, cor_dict)

    df = enrich_proximite(df, cor_dict)

    df = enrich_pays(df, cor_dict)

    df = enrich_ed(df, cor_dict)

    df = enrich_iut(df, cor_dict)

    df = enrich_ing(df, cor_dict)

    df = enrich_lmd(df, cor_dict)

    df = enrich_dndu(df, cor_dict)

    df = enrich_dutbut(df, cor_dict)

    df = autres_multi(df)

    # ##########################
    end = time.time()
    print(f"fin enrich -> {end - start}", flush=True)
    start = time.time()
    # ###########################

    print("lmddont/niveau/prox/deptoreg", flush=True)
    df = LMDdont(df)

    gd_disc = pd.DataFrame(cor_dict['DISCIPLINES_SISE'])[['GDDISC', 'SECTDIS']]

    df = pd.merge(df, gd_disc, how='left', on='SECTDIS')

    df = niveau_retard_avance(df)

    df = proximite_correctifs(df)

    df = deptoreg(df, cor_dict)

    df = corrige2018_2024(df)

    ####################################
    end = time.time()
    print(f"fin lmddont/niveau/prox/deptoreg -> {end - start}", flush=True)
    start = time.time()
    ####################################

    print(f"size df_cleaned after all corrections-> {df.shape}", flush=True)

    typ_str = ['COMETAB', 'DEPRESPA', 'ACARESPA', 'COMINS', 'ANNAIS', 'ANBAC', 'AGE_BAC']
    for t in typ_str:
        df[t] = df[t].astype(str)
        df[t] = df[t].str.split('.', regex=False).str[0].str.strip()
        df[t] = df[t].str.replace('nan', '', regex=False)
        df[t] = df[t].str.replace('<NA>', '', regex=False)

    typ_num = ['NBACH', 'NET']
    for t in typ_num:
        df[t] = df[t].astype(pd.Int64Dtype())
        df.loc[(df[t].isnull()), t] = 0

    geo = ['BRICS', 'OCDE_MEMBRES', 'OCDE_OBS', 'UE_27', 'UE_28', 'UE_EURO', 'BOLOGNE']
    for g in geo:
        df[g] = df[g].astype(pd.Int64Dtype())

    df[df.select_dtypes("object").columns] = df.select_dtypes("object").fillna("")

    ####################################
    end = time.time()
    print(f"fin calage modele -> {end - start}", flush=True)
    start = time.time()
    #################################################

    if "RESINT" in df.columns:
        df.loc[(df["RESDIP"] == "O") & (df["RESINT"].isin(["N", "X"])), ["EFFECTIF_INT", "TYP_DIPINT", "DIPINT",
                                                                         "CURSUS_LMD_INT", "SECTINT", "DISCIPLINT",
                                                                         "GROUPINT", "LMDDONTBIS_INT"]] = [0, "", "",
                                                                                                           "",
                                                                                                           "", "", "",
                                                                                                           ""]

    if source == "inge":
        df.loc[(df["RESDIP"] == "O") & (df["DIPLOM"] == "6000361") & (df["TYP_DIPL"] == "99"), "CURSUS_LMDR"] = "M"
        df.loc[(df["RESDIP"] == "O") & (df["DIPLOM"] == "6000361") & (df["TYP_DIPL"] == "99"), "SECTDIS"] = "16"
        df.loc[(df["RESDIP"] == "O") & (df["DIPLOM"] == "6000361") & (df["TYP_DIPL"] == "99"), "DISCIPLI"] = "15"
        df.loc[(df["RESDIP"] == "O") & (df["DIPLOM"] == "6000361") & (df["TYP_DIPL"] == "99"), "TYP_DIPL"] = "FI"
        if "RESINT" in df.columns and rentree == 2020:
            df.loc[
                (df["RESDIP"] == "O") & (df["RESINT"] == "N") & (df["DIPLOM"].isin(["2500203", "2500164"])) & (
                    df["DIPINT"].isin(["", "None", None, np.nan])) & (
                    df["TYP_DIPINT"].isin(["", "None", None, np.nan])) & (
                    ~df["LMDDONT_INT"].isin(["", "None", None, np.nan])) & (
                    df["ETABLI_ORI_UAI"].isin(["0597131F", "0632033T"])), "suppression"] = "suppression"
            df = df.loc[df["suppression"].isna()].drop(columns="suppression")

    if source == "result":
        df.loc[(df["RESDIP"] == "O") & (df["RESINT"] == "X") & (df["DIPINT"] == "8"), "TYP_DIPINT"] = ""
        df.loc[(df["RESDIP"] == "O") & (df["RESINT"] == "X") & (df["DIPINT"] == "8"), "DISCIPLINT"] = ""
        df.loc[(df["RESDIP"] == "O") & (df["RESINT"] == "X") & (df["DIPINT"] == "8"), "SECTINT"] = ""

    df.loc[(df["CURSUS_LMDR"] != "D") & (df["ID_PAYSAGE_ED"] != ""), "ID_PAYSAGE_ED"] = ""
    df.loc[(df["TYP_DIPL"] == "JD") & (df["CURSUS_LMDR"] == "L"), "CURSUS_LMDR"] = "M"

    df["EFFECTIF_TOT"] = 0

    if "RESINT" not in df.columns:
        df["EFFECTIF_TOT"] = df["EFFECTIF_R"].copy()
    else:
        df["EFFECTIF_TOT"] = df["EFFECTIF_R"] + df["EFFECTIF_INT"]

    df = df.loc[df["EFFECTIF_TOT"] > 0]

    print(f"duration cleaning {source}{rentree} -> {start - start_main}", flush=True)

    return df


def chunkify(df: pd.DataFrame, chunk_size: int):
    print(f"size df: {df.shape}")
    start = 0
    length = df.shape[0]

    # If DF is smaller than the chunk, return the DF
    if length <= chunk_size:
        yield df[:]
        return

    # Yield individual chunks
    while start + chunk_size <= length:
        yield df[start:chunk_size + start]
        start = start + chunk_size

    # Yield the remainder chunk, if needed
    if start < length:
        yield df[start:]


def corrige(cor_dict):
    logger.debug(f'start correction')
    DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")

    for year in range(2015, 2022):
        logger.debug(f'start correction for {year}')
        sources = get_sources(year)
        for source in sources:
            logger.debug(f'start correction for {year} {source}')
            df = read_diplome(source, year)
            filename = get_filename(source, year)
            df.loc[df["DIPLOMR"].isin(["", "None", None]), "DIPLOMR"] = np.nan
            if "DIPINT" in df.columns:
                df.loc[df["DIPINT"].isin(["", "None", None]), "DIPINT"] = np.nan
                df.loc[df["TYP_DIPINT"].isin(["", "None", None]), "TYP_DIPINT"] = np.nan
                df.loc[(df["DIPLOMR"].isna()) & (df["TYP_DIPR"] == "99") & (df["DIPINT"].isna()) & (
                    df["TYP_DIPINT"].isna()), "remove"] = "remove"
                df = df.loc[df["remove"] != "remove"]
                df = df.drop(columns="remove")
            else:
                df.loc[(df["DIPLOMR"].isna()) & (df["TYP_DIPR"] == "99"), "remove"] = "remove"
                df = df.loc[df["remove"] != "remove"]
                df = df.drop(columns="remove")

            if "NIVEAUR" in df.columns:
                df = df.drop(
                    columns=["SECTDIS", "COMPOS", "DISCIPLI", "DIPLOM", "CYCLE", "GROUPE", "NIVEAU", "TYP_DIPL",
                             "VOIE"])
                df = df.rename(
                    columns={"SECTDISR": "SECTDIS", "COMPOSR": "COMPOS", "DISCIPLR": "DISCIPLI", "DIPLOMR": "DIPLOM",
                             "CYCLER": "CYCLE", "GROUPER": "GROUPE", "NIVEAUR": "NIVEAU", "TYP_DIPR": "TYP_DIPL",
                             "VOIER": "VOIE"})
            else:
                df = df.drop(
                    columns=["SECTDIS", "COMPOS", "DISCIPLI", "DIPLOM", "CYCLE", "GROUPE", "TYP_DIPL", "VOIE"])
                df = df.rename(
                    columns={"SECTDISR": "SECTDIS", "COMPOSR": "COMPOS", "DISCIPLR": "DISCIPLI", "DIPLOMR": "DIPLOM",
                             "CYCLER": "CYCLE", "GROUPER": "GROUPE", "TYP_DIPR": "TYP_DIPL", "VOIER": "VOIE"})

            if "FLAG_MEEF" in df.columns:
                df["FLAG_MEEF"] = df["FLAG_MEEF"].astype(str)

            data_chunks = list(chunkify(df, 500000))
            tmp = list()
            start_main = time.time()
            for i in range(0, len(data_chunks)):
                print(f"Loop {i + 1}, size data_chunks: {len(data_chunks)}")
                df_temp = data_chunks[i]
                df_temp = corrige_df(df_temp, source, year, cor_dict)
                tmp.append(df_temp)
            corrected_df = pd.concat(tmp, ignore_index=True)

            print(f"duration cleaning {source}{year} -> {time.time() - start_main}")
            filename2 = filename.replace(".parquet", "")
            to_parquet(corrected_df, f'corrected_{filename2}')
            swift.upload_object_path("sas", f'{DATA_PATH}parquet/corrected_{filename2}.parquet')
    logger.debug('done')
