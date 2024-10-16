import time

from cleaning_functions import *
import dtype_diplomes as types

logger = get_logger(__name__)

pd.options.mode.chained_assignment = None


def corrige_df(df, source, rentree, cor_dict):
    start_main = time.time()
    ##############################################
    print("init_cal_var", flush=True)
    df = init_cal_var(df)

    for col in list(df.select_dtypes("object").columns):
        df[col] = df[col].str.strip()

    for v in ['NBACH', 'NET', 'EFFECTIF', 'FLAG_SUP', 'ANNAIS', 'ANBAC', 'ANSUP', 'FLAG_MEEF']:
        if v in df.columns:
            df[v] = df[v].astype(pd.Int64Dtype())

    df_float = list(df.select_dtypes(include=[float]).columns)
    if len(df_float) != 0:
        print(f"ATTENTION ! liste des vars float restantes après traitement -> {df_float}", flush=True)

    df.loc[(df.EFFECTIF == '') | (df.EFFECTIF.isnull()), 'EFFECTIF'] = 0  # A vérifier quand le pb se présentera
    if any(~df.EFFECTIF.isin([1, 0])):
        raise TypeError(f"EFFECTIF, attention valeur non attendue  -> {df.EFFECTIF.unique()}")
    else:
        df.EFFECTIF = df.EFFECTIF.astype(int)

    print("COMPOS", flush=True)
    df = fill_COMPOS(df)
    print("NUMED", flush=True)
    df = corrige_NUMED(df)

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

    ########################################
    end = time.time()
    print(f"fin delete -> {end - start}", flush=True)
    start = time.time()
    #######################################

    mask_priv = ((df['ID_PAYSAGE_FORMENS'] == '') | (pd.isna(df['ID_PAYSAGE_FORMENS'])) | (
        df['ID_PAYSAGE_FORMENS'].isnull())) & (df['SOURCE'] == 'priv')
    df.loc[mask_priv, 'FLAG_MEEF'] = 1
    df.loc[mask_priv, 'EFF_SS_ESPE'] = 0

    df.loc[(df.RENTREE == '2010') & (df.COMPOS == "9830491S"), 'UI'] = "9830445S"
    df.loc[df.COMPOS == "0684045X", 'UI'] = "0694045X"

    df = df.loc[~((df.SOURCE == 'inscri') & (df.RENTREE == '2011') & (df.DIPLOM == "9010371"))]

    df.loc[df.COMPOS == "0161192P", 'COMPOS'] = "0161192J"

    df.loc[(df.COMPOS == '0751988D') & (df.ETABLI.isin(["0751720M", "0755736C"])) & (df.EFFECTIF == 0) & (
            df.INSPR == "O"), 'INSPR'] = "N"

    print("read rattach", flush=True)
    rattach = read_rattach(rentree)
    df = df.merge(rattach, how='left', on='COMPOS')
    df['UR'] = np.where((df['RATTACH'] == '') | (df['RATTACH'].isnull()), df['COMPOS'], df['RATTACH'])

    df.loc[df.COMPOS == "0011312W", 'UR'] = "0694121E"
    df.loc[df.COMPOS == "0133479L", 'UR'] = "0131844J"

    ########################################
    end = time.time()
    print(f"fin rattach -> {end - start}", flush=True)
    start = time.time()
    #########################################
    print("etabli source", flush=True)
    df = corrige_ETABLI_SOURCE(df, cor_dict)
    print("etabli", flush=True)
    df = corrige_ETABLI(df, cor_dict)
    print("enrich_a_uai", flush=True)
    df = enrich_a_uai(df, cor_dict)
    print("enrich_d_epe", flush=True)
    df = enrich_d_epe(df, cor_dict)
    print("etabli diffusion", flush=True)
    df = corrige_ETABLI_DIFFUSION(df, cor_dict)
    print("comins", flush=True)
    df = corrige_COMINS(df, cor_dict)

    # ###########################################
    end = time.time()
    print(f"fin etabli/comins -> {end - start}", flush=True)
    start = time.time()
    # ############################################

    # lignes 950
    df.loc[(df.COMPOS == "0332910J") & (df.COMINS == "33063"), 'COMPOS'] = "0332969Y"
    # lignes 978
    print("cometab", flush=True)
    cometab_form = read_cometab(rentree)
    # COMINS
    df = df.merge(cometab_form, how='left', left_on='COMPOS', right_on='UAI').rename(
        columns={'COMCODE': 'COMINS2'}).drop(columns='UAI').reset_index().drop(columns="index")
    df.loc[(df["COMINS"] == "") | (df["COMINS"].isna()), "COMINS"] = df.loc[
        (df["COMINS"] == "") | (df["COMINS"].isna()), "COMINS2"]
    df = df.drop(columns="COMINS2")
    # COMETAB
    df = df.merge(cometab_form, how='left', left_on='ETABLI', right_on='UAI').drop(columns={'UAI'}).reset_index().drop(
        columns="index")

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

    # #################################
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

    # # SI NIVEAU et SECTDIS = 1 cracatere numerique alors ajouter un '0' devant*/
    for c in ['NIVEAU', 'SECTDIS', 'DISCIPLI', 'TYP_DIPL']:
        mask = (df[c].str.len() == 1) & (df[c].str.isnumeric())
        df.loc[mask, c] = df.loc[mask, c].str.rjust(2, fillchar='0')

    df.loc[df.SECTDIS.isin(['44', '45', '46', '47', '48']), 'SECTDIS'] = "39"

    print("cursusLMD/groupe", flush=True)
    df = corrige_cursus_lmd(df, cor_dict)
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
    df = effectifs(df)

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
    df = corrige2018_2124(df)

    ####################################
    end = time.time()
    print(f"fin lmddont/niveau/prox/deptoreg -> {end - start}", flush=True)
    start = time.time()
    ####################################

    print(f"size df_cleaned after all corrections-> {df.shape}", flush=True)
    print("cal var", flush=True)
    df = cal_var(df, 'end')

    typ_str = ['COMETAB', 'DEPRESPA', 'ACARESPA', 'COMINS', 'CP_ETU', 'CP_PAR', 'ANNAIS', 'ANBAC', 'ANSUP', 'AGE_BAC']
    for t in typ_str:
        df[t] = df[t].astype(str)
        df[t] = df[t].str.split('.', regex=False).str[0].str.strip()
        df[t] = df[t].str.replace('nan', '', regex=False)
        df[t] = df[t].str.replace('<NA>', '', regex=False)

    typ_num = ['EFFECTIF', 'EFFECTIF_CESURE', 'EFFS_SS_CPGE', 'EFFT_SS_CPGE', 'EFF_SS_CPGE', 'EFFS', 'EFFT',
               'NBACH', 'NBACH_CESURE', 'NBACH_SS_CPGE', 'NET']
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
    for year in range(2004, 2022):
        logger.debug(f'start correction for {year}')
        sources = get_sources(year)
        for source in sources:
            logger.debug(f'start correction for {year} {source}')
            filename = get_filename(source=source, rentree=year)
            df = read_sise(source=source, rentree=year)
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
            to_parquet(corrected_df, f'corrected_{filename}')
    logger.debug('done')
