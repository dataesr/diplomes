import sys

import numpy as np
import pandas as pd

from application.server.main.logger import get_logger

logger = get_logger(__name__)

pd.options.mode.chained_assignment = None

ALL_RENTREES = list(range(2015, 2022))
ALL_TYPES = ['result', 'inge', 'priv', 'ens', 'mana', 'enq26bis', 'culture']


def fill_COMPOS(df):
    mask_etabli_ok = ~(df['ETABLI'].str.len().isna())
    mask_compos_empty = df['COMPOS'].str.len().isna()
    mask_compos_small = df['COMPOS'].str.len() < 3
    mask = mask_etabli_ok & (mask_compos_empty | mask_compos_small)
    df.loc[mask, 'COMPOS'] = df['ETABLI']
    return df


def corrige_NUMED(df):
    if 'NUMED' in df.columns:
        df['NUMED'] = df['NUMED'].str.removeprefix('0')
        df.loc[df['NUMED'] == 'HORS', 'NUMED'] = ''
    else:
        df['NUMED'] = ''
    return df


def corrige_ETABLI_SOURCE(df, cor_dic):
    data_etabli = []
    data_compos = []
    for c in cor_dic['ETABLI_SOURCE']:
        assert (c['OUT'])
        sources = ALL_TYPES
        if c['SOURCE']:
            sources = [c['SOURCE']]
        rentrees = []
        if c['RENTREE_START']:
            for y in range(int(c['RENTREE_START']), int(c['RENTREE_END']) + 1):
                rentrees.append(str(y))
        else:
            rentrees = [str(y) for y in ALL_RENTREES]
        for source in sources:
            for rentree in rentrees:
                if c['IN']:
                    data_etabli.append(
                        {'RENTREE': rentree, 'SOURCE': source, 'ETABLI': c['IN'], 'ETABLI_NEW': c['OUT']})
                elif c['COMPOS']:
                    data_compos.append({'RENTREE': rentree, 'SOURCE': source, 'COMPOS': c['COMPOS'],
                                        'RATTACH': c['COMPOS'],
                                        'ETABLI_NEW': c['OUT']})
    df_etabli = pd.DataFrame(data_etabli)
    df_compos = pd.DataFrame(data_compos)
    del df_compos['RATTACH']
    df_rattach = pd.DataFrame(data_compos)
    del df_rattach['COMPOS']

    if 'ETABLI' in df.columns:
        df = pd.merge(df, df_etabli, on=['RENTREE', 'SOURCE', 'ETABLI'], how='left')
        mask = df['ETABLI_NEW'].str.len() > 3
        df.loc[mask, 'ETABLI'] = df['ETABLI_NEW']
        del df['ETABLI_NEW']

    if 'COMPOS' in df.columns:
        df = pd.merge(df, df_compos, on=['RENTREE', 'SOURCE', 'COMPOS'], how='left')
        mask = df['ETABLI_NEW'].str.len() > 3
        df.loc[mask, 'ETABLI'] = df['ETABLI_NEW']
        del df['ETABLI_NEW']

    if 'RATTACH' in df.columns:
        df = pd.merge(df, df_rattach, on=['RENTREE', 'SOURCE', 'RATTACH'], how='left')
        mask = df['ETABLI_NEW'].str.len() > 3
        df.loc[mask, 'ETABLI'] = df['ETABLI_NEW']
        del df['ETABLI_NEW']
        del df['RATTACH']

    return df


def corrige_ETABLI(df, cor_dic):
    data_etabli = []
    for c in cor_dic['ETABLI']:
        assert (c['OUT'])
        rentrees = []
        if c['RENTREE_START']:
            for y in range(int(c['RENTREE_START']), int(c['RENTREE_END']) + 1):
                rentrees.append(str(y))
        else:
            rentrees = [str(y) for y in ALL_RENTREES]
        for rentree in rentrees:
            if c['IN']:
                data_etabli.append({'RENTREE': rentree, 'ETABLI': c['IN'], 'ETABLI_NEW': c['OUT']})

        df_etabli = pd.DataFrame(data_etabli)

    if 'ETABLI' in df.columns:
        df = pd.merge(df, df_etabli, on=['RENTREE', 'ETABLI'], how='left')
        mask = df['ETABLI_NEW'].str.len() > 3
        df.loc[mask, 'ETABLI'] = df['ETABLI_NEW']
        del df['ETABLI_NEW']
    return df


def aplatir(conteneurs):
    return [conteneurs[i][j] for i in range(len(conteneurs))
            for j in range(len(conteneurs[i]))]


def delete(df, cor_dic):
    for c in cor_dic['deleter']:
        if c['ETABLI_DIFFUSION'] != '':
            df = df[~((df['RENTREE'] == c['RENTREE']) & (df['ETABLI_DIFFUSION'] == c['ETABLI_DIFFUSION']) & (
                    df['SOURCE'] == c['SOURCE']))]
        elif c['ETABLI_ORI_UAI'] != '':
            df = df[~((df['RENTREE'] == c['RENTREE']) & (df['DIPLOM'] == c['DIPLOM']) & (
                    df['SOURCE'] == c['SOURCE']) & (df['ETABLI_ORI_UAI'] == c['ETABLI_ORI_UAI']))]
        elif c['ETABLI'] != '':
            df = df[~((df['RENTREE'] == c['RENTREE']) & (df['ETABLI'] == c['ETABLI']) & (df['SOURCE'] == c['SOURCE']))]
        else:
            df = df[~((df['RENTREE'] == c['RENTREE']) & (df['DIPLOM'] == c['DIPLOM']) & (df['SOURCE'] == c['SOURCE']))]
    return df


def corrige_ETABLI_DIFFUSION(df, cor_dict):
    if 'FLAG_MEEF' in df.columns:
        mask = (df.RENTREE == '2014')
        df.loc[mask & (df.ETABLI_DIFFUSION == "UNIVERSITE NELLE CALEDONIE") & (
                df.FLAG_MEEF == "1"), 'ETABLI_DIFFUSION'] = "ESPE ACADEMIE DE NOUVELLE CALE"
        df.loc[mask & (df.ETABLI_DIFFUSION == "ESPE ACADEMIE DE NOUVELLE CALE") & (
                df.FLAG_MEEF == "0"), 'ETABLI_DIFFUSION'] = "UNIVERSITE NELLE CALEDONIE"
    df.loc[(df.ETABLI_DIFFUSION == "ESPE ACADEMIE DE REUNION") & (
            df.TYP_DIPL == "VF"), 'ETABLI_DIFFUSION'] = "UNIVERSITE LA REUNION"

    df_etab = pd.DataFrame(cor_dict['ETABLI_DIFFUSION_ID'])
    df = df.merge(df_etab, how='left', left_on='ETABLI_DIFFUSION', right_on='IN').drop(columns='IN')
    df['ID_PAYSAGE_FORMENS'] = np.where(df.OUT != '', df.OUT, df.ID_PAYSAGE_FORMENS)
    df = df.drop(columns='OUT')
    return df


def corrige_COMINS(df, cor_dic):
    VAR = 'COMINS'
    for c in cor_dic['COMINS']:
        if c['TYP_DIPL'] != 'rien':
            df.loc[(df['TYP_DIPL'] == c['TYP_DIPL']) & (df['COMPOS'] == c['COMPOS']) & (
                    df['RENTREE'] == c['RENTREE']), 'COMINS'] = c['OUT']
        else:
            if c['COMPOS'] != 'rien':
                if c['RENTREE'] != 'rien':
                    if c['IN'] != 'rien':
                        if c['IN'] == 'crochet':
                            df.loc[((df[VAR] == '') | (pd.isna(df[VAR])) | (df[VAR] == None) | (df[VAR] == 'None')) & (
                                    df['COMPOS'] == c['COMPOS']) & (df['RENTREE'] == c['RENTREE']), 'COMINS'] = c[
                                'OUT']
                        else:
                            df.loc[(df[VAR] == c['IN']) & (df['COMPOS'] == c['COMPOS']) & (
                                    df['RENTREE'] == c['RENTREE']), 'COMINS'] = c['OUT']
                    else:
                        df.loc[(df['COMPOS'] == c['COMPOS']) & (df['RENTREE'] == c['RENTREE']), 'COMINS'] = c['OUT']
                else:
                    if c['IN'] != 'rien':
                        df.loc[(df[VAR] == c['IN']) & (df['COMPOS'] == c['COMPOS']), 'COMINS'] = c['OUT']
                    else:
                        df.loc[(df['COMPOS'] == c['COMPOS']), 'COMINS'] = c['OUT']
            else:
                df.loc[(df[VAR] == c['IN']), 'COMINS'] = c['OUT']
    return df


def corrige_COMETAB(df, cor_dic):
    VAR = 'COMETAB'
    for c in cor_dic['COMETAB']:
        if c['ETABLI'] != 'rien':
            if c['IN'] == 'rien':
                df.loc[(df['ETABLI'] == c['ETABLI']) & (df['RENTREE'] == c['RENTREE']), 'COMETAB'] = c['OUT']
            else:
                df.loc[
                    (pd.isna(df[VAR])) & (df['ETABLI'] == c['ETABLI']) & (df['RENTREE'] == c['RENTREE']), 'COMETAB'] = \
                    c['OUT']
        else:
            df.loc[(df['ID_PAYSAGE'] == c['ID_PAYSAGE']) & (df['RENTREE'] == c['RENTREE']), 'COMETAB'] = c['OUT']
    return df


def corrige_COMUI(df, cor_dic):
    VAR = 'COMUI'
    df['UI_5'] = df['UI'].str[:5]
    for c in cor_dic['COMUI']:
        if c['IN'] == 'crochet':
            df.loc[((df[VAR] == '') | (pd.isna(df[VAR])) | (df[VAR] == None)) & (df['COMPOS'] == c['COMPOS']) & (
                    df['RENTREE'] == c['RENTREE']), 'COMUI'] = c['OUT']
        else:
            df.loc[(df[VAR] == c['IN']) & (df['COMPOS'] == c['COMPOS']) & (df['RENTREE'] == c['RENTREE']), 'COMUI'] = c[
                'OUT']
    del df['UI_5']
    return df


def corr_single_var(fl, df):
    for _, r in fl.iterrows():
        if r.VAR1_IN in df.columns:
            df.loc[df[r.VAR1_IN] == r.VALUE1_IN, r.VAR_OUT] = r.VALUE_OUT
    return df


def corr_2_var(fl, df):
    for _, r in fl.iterrows():
        if r.VAR1_IN in df.columns:
            if r.VAR2_IN != "" or r.VAR2_IN in df.columns:
                df.loc[(df[r.VAR1_IN] == r.VALUE1_IN) & (df[r.VAR2_IN].isna()), r.VAR_OUT] = r.VALUE_OUT

    return df


def corr_reste_groupe(name, df, cor_dic):
    feuille = pd.DataFrame(cor_dic[name])
    feuille_nvar2 = feuille.loc[feuille["VALUE2_IN"] == ""]
    df = corr_single_var(feuille_nvar2, df)
    feuille_var2 = feuille.loc[feuille["VALUE2_IN"] != ""]
    df = corr_2_var(feuille_var2, df)
    return df


def corr_dep_aca(name, var, df, cor_dic):
    feuille = pd.DataFrame(cor_dic[name])
    feuille = feuille[feuille['VAR_OUT'] == var]
    df = corr_single_var(feuille, df)
    return df


def corr_single_var_if(fl, df):
    for _, r in fl.iterrows():
        if "RENTREE" in df.columns and r.VAR_IN in df.columns:
            df.loc[(df["RENTREE"] == r.RENTREE) & (df[r.VAR_IN] == r.VALUE_IN), r.VAR_OUT] = r.VALUE_OUT
    return df


def corr_dep_aca_respa(name, df, cor_dic):
    feuille = pd.DataFrame(cor_dic[name])
    df = corr_single_var_if(feuille, df)
    return df


def corrige_FORMATIONS(df, cor_dic):
    df_corrform = cor_dic['FORMATIONS_CORRECTIF']

    if 'TYP_DIPINT' in df.columns:
        cols = ['NIVEAU', 'SECTDIS', 'DISCIPLI', 'TYP_DIPL', 'TYP_DIPINT']
    else:
        cols = ['NIVEAU', 'SECTDIS', 'DISCIPLI', 'TYP_DIPL']

    for c in cols:
        mask = (df[c].str.len() == 1) & (df[c].str.isnumeric())
        df.loc[mask, c] = df.loc[mask, c].str.rjust(2, fillchar='0')

    for i in df_corrform:
        dict_in = {'keys_in': [], 'values_in': [], 'len': [], 'keys_out': [], 'values_out': []}
        for k, v in i.items():
            if v != '' and '_IN' in k:
                k = k.rsplit('_', 1)[0].strip()
                v = v.replace('[empty]', '')
                dict_in['keys_in'].append(k)
                dict_in['values_in'].append(v)
            if v != '' and '_OUT' in k:
                k = k.rsplit('_', 1)[0].strip()
                dict_in['keys_out'].append(k)
                dict_in['values_out'].append(v)

        dict_in['len'] = len(dict_in['keys_in'])
        key = dict_in["keys_out"]
        value = dict_in['values_out']
        for col in dict_in['keys_in']:
            df[col + '_temp'] = df[col].copy(deep=True)
        for i in range(len(key)):
            if dict_in['len'] == 2:
                df.loc[(df[dict_in['keys_in'][0] + "_temp"] == dict_in['values_in'][0]) & (
                        df[dict_in['keys_in'][1] + "_temp"] == dict_in['values_in'][1]), key[i]] = value[i]
            elif dict_in['len'] == 3:
                df.loc[(df[dict_in['keys_in'][0] + "_temp"] == dict_in['values_in'][0]) & (
                        df[dict_in['keys_in'][1] + "_temp"] == dict_in['values_in'][1]) & (
                               df[dict_in['keys_in'][2] + "_temp"] == dict_in['values_in'][2]), key[i]] = value[i]
            elif dict_in['len'] > 3:
                print('ATTENTION faire code pour corrections formation -> plus de 3 variables sous condition',
                      flush=True)
        df = df.loc[:, ~df.columns.str.contains('_temp')]
    return df


def corrige_cursus_lmd(df, cor_dic):
    df_corrlmd = pd.DataFrame(cor_dic['CORRLMD'])

    if 'CURSUS_LMD' in df.columns:
        df = pd.merge(df, df_corrlmd, how='left', on='TYP_DIPL')
        mask = (df['CURSUS_LMD'].isin(['nan', '', 'X'])) & (df['CURSUS_LMD_OUT'].str.len() > 0)
        df.loc[mask, 'CURSUS_LMD'] = df.loc[mask, 'CURSUS_LMD_OUT']
        df = df.drop('CURSUS_LMD_OUT', axis=1)
        if "TYP_DIPINT" in df.columns:
            df_corrlmd = df_corrlmd.rename(columns={'TYP_DIPL': 'TYP_DIPINT'})
            df = pd.merge(df, df_corrlmd, how='left', left_on='TYP_DIPINT', right_on="TYP_DIPINT")
            df.loc[df['CURSUS_LMD_OUT'].str.len() > 0, 'CURSUS_LMD_INT'] = df.loc[
                df['CURSUS_LMD_OUT'].str.len() > 0, 'CURSUS_LMD_OUT']
            df = df.drop('CURSUS_LMD_OUT', axis=1)

    df_corrcursus = pd.DataFrame(cor_dic['CURSUS_LMD_CORRECTIF'])
    if 'CURSUS_LMD' in df.columns:
        use = df_corrcursus.loc[(df_corrcursus['NIVEAU'] != '')]
        df = pd.merge(df, use, how='left', on=['TYP_DIPL', 'DIPLOM', 'NIVEAU'])
        mask = (df['CURSUS_LMD_OUT'].str.len() > 0)
        df.loc[mask, 'CURSUS_LMD'] = df.loc[mask, 'CURSUS_LMD_OUT']
        df = df.drop('CURSUS_LMD_OUT', axis=1)
        if "TYP_DIPINT" in df.columns:
            use = df_corrcursus.loc[(df_corrcursus['NIVEAU'] != '')]
            use = use.rename(columns={'TYP_DIPL': 'TYP_DIPINT'})
            df = pd.merge(df, use, how='left', on=['TYP_DIPINT', 'DIPLOM', 'NIVEAU'])
            mask = (df['CURSUS_LMD_OUT'].str.len() > 0)
            df.loc[mask, 'CURSUS_LMD_INT'] = df.loc[mask, 'CURSUS_LMD_OUT']
            df = df.drop('CURSUS_LMD_OUT', axis=1)

        use = df_corrcursus.loc[(df_corrcursus['NIVEAU'] == '')].drop(columns={'NIVEAU'})
        df = pd.merge(df, use, how='left', on=['TYP_DIPL', 'DIPLOM'])
        mask = (df['CURSUS_LMD_OUT'].str.len() > 0)
        df.loc[mask, 'CURSUS_LMD'] = df.loc[mask, 'CURSUS_LMD_OUT']
        df = df.drop('CURSUS_LMD_OUT', axis=1)
        if "TYP_DIPINT" in df.columns:
            use = use.rename(columns={'TYP_DIPL': 'TYP_DIPINT'})
            df = pd.merge(df, use, how='left', on=['TYP_DIPINT', 'DIPLOM'])
            mask = (df['CURSUS_LMD_OUT'].str.len() > 0)
            df.loc[mask, 'CURSUS_LMD_INT'] = df.loc[mask, 'CURSUS_LMD_OUT']
            df = df.drop('CURSUS_LMD_OUT', axis=1)

    return df


def enrich_a_uai(df, cor_dic):
    df_uai = pd.DataFrame(cor_dic['A_UAI'])
    a_uai = df_uai.loc[df_uai["TYPE"] == "result"]
    a_uai2 = a_uai.copy()
    a_uai = a_uai.drop(columns="ID_PAYSAGE").drop_duplicates().reset_index(drop=True)
    a_uai3 = pd.merge(a_uai, a_uai2, on=["RENTREE", "ANNEE", "TYPE", "SOURCE", "ETABLI"], how="outer")
    if len(a_uai3) == len(a_uai2) == len(a_uai):
        if 'ETABLI' in df.columns:
            df = pd.merge(df, df_uai.loc[df_uai["TYPE"] == "result", ['RENTREE', 'SOURCE', 'ETABLI', 'ID_PAYSAGE']],
                          on=['RENTREE', 'SOURCE', 'ETABLI'],
                          how='left')
            if len(df.loc[df["ID_PAYSAGE"].isna()]) > 0:
                df_na = df.loc[df["ID_PAYSAGE"].isna()]
                df_na = df_na.drop(columns="ID_PAYSAGE")
                df_na = pd.merge(df_na, df_uai.loc[
                    df_uai["TYPE"] == "inscri", ['RENTREE', 'SOURCE', 'ETABLI', 'ID_PAYSAGE']],
                                 on=['RENTREE', 'SOURCE', 'ETABLI'],
                                 how='left')
                dfnna = df.loc[df["ID_PAYSAGE"].notna()]
                df = pd.concat([dfnna, df_na], ignore_index=True)
                return df
    else:
        logger.debug("Doublons dans A_UAI")
        sys.exit(1)




def enrich_d_epe(df, cor_dic):
    df_epe = pd.DataFrame(cor_dic['D_EPE'])
    df_epe['ID_PAYSAGE_EPE_ETAB_COMPOS'] = df_epe['ID_PAYSAGE']
    if 'ID_PAYSAGE' in df.columns:
        df = pd.merge(df, df_epe[['RENTREE', 'ID_PAYSAGE_EPE_ETAB_COMPOS', 'ID_PAYSAGE_EPE']],
                      left_on=['RENTREE', 'ID_PAYSAGE'], right_on=['RENTREE', 'ID_PAYSAGE_EPE_ETAB_COMPOS'], how='left')
        df['ID_PAYSAGE'] = np.where((df.ID_PAYSAGE_EPE == '') | (df.ID_PAYSAGE_EPE.isnull()), df['ID_PAYSAGE'],
                                    df['ID_PAYSAGE_EPE'])
    return df


def enrich_les_communes(df, cor_dic):
    df_communes = pd.DataFrame(cor_dic['LES_COMMUNES'])
    df_communes['DEPINS'] = df_communes['DEP_ID'].str[1:4]
    df_communes['UUCRINS'] = df_communes['UUCR_ID']
    df_communes['COMUI'] = df_communes['COM_CODE']
    if 'COMUI' in df.columns:
        df = pd.merge(df, df_communes[['DEPINS', 'UUCRINS', 'COMUI']], on='COMUI', how='left')
    df_communes['UUCRETAB'] = df_communes['UUCR_ID']
    df_communes['COMETAB'] = df_communes['COM_CODE']
    if 'COMETAB' in df.columns:
        df = pd.merge(df, df_communes[['UUCRETAB', 'COMETAB']], on='COMETAB', how='left')
    return df


def enrich_proximite(df, cor_dic):
    df_proximite = pd.DataFrame(cor_dic['H_PROXIMITE'])
    df_proximite['DEPINS'] = df_proximite['DEPARTEMENT_UI']
    df_proximite['DEPRESPA'] = df_proximite['DEPARTEMENT_PARENTS']
    df_proximite['PROXIMITE'] = df_proximite['PROXIMIT_']
    df_proximite['PROXREG'] = df_proximite['PROX_R_GIONS']
    if 'DEPINS' in df.columns and 'DEPRESPA' in df.columns:
        df = pd.merge(df, df_proximite[['DEPINS', 'DEPRESPA', 'PROXREG', 'PROXIMITE']],
                      on=['DEPINS', 'DEPRESPA'], how='left')

    df_proximite['PROXBAC'] = df_proximite['PROXIMIT_']
    df_proximite['PROXREGBAC'] = df_proximite['PROX_R_GIONS']
    df_proximite['DEPBAC'] = df_proximite['DEPARTEMENT_PARENTS']
    if 'DEPINS' in df.columns and 'DEPBAC' in df.columns:
        df = pd.merge(df, df_proximite[['DEPINS', 'DEPBAC', 'PROXBAC', 'PROXREGBAC', 'OUTREMER']],
                      on=['DEPINS', 'DEPBAC'], how='left')
    return df


def enrich_pays(df, cor_dic):
    df_pays = pd.DataFrame(cor_dic['G_PAYS'])
    df_pays['NATION'] = df_pays['PAYS']
    c999 = list(df.loc[~df["NATION"].isin(df_pays["NATION"]), "NATION"])
    df.loc[df["NATION"].isin(c999), "NATION"] = "999"
    if 'NATION' in df.columns:
        df = pd.merge(df, df_pays[['NATION', 'CONTINENT', 'UE_28', 'UE_27', 'UE_EURO',
                                   'OCDE_MEMBRES', 'OCDE_OBS', 'BOLOGNE', 'BRICS']],
                      on='NATION', how='left')
        for col in ['UE_28', 'UE_27', 'UE_EURO', 'OCDE_MEMBRES', 'OCDE_OBS', 'BOLOGNE', 'BRICS']:
            df[col] = df[col].astype(int)

    return df


def enrich_ed(df, cor_dic):
    df_ed = pd.DataFrame(cor_dic['L_ED'])
    if 'NUMED' in df.columns:
        df = pd.merge(df, df_ed[['NUMED', 'ID_PAYSAGE_ED']], on='NUMED', how='left')
    return df


def enrich_iut(df, cor_dic):
    df_iut = pd.DataFrame(cor_dic['M_IUT'])
    if 'UR' in df.columns and 'UI' in df.columns:
        df = pd.merge(df,
                      df_iut[['RENTREE', 'UR', 'UI', 'ID_PAYSAGE_IUT', 'ID_PAYSAGE_IUT_CAMPUS', 'ID_PAYSAGE_IUT_POLE']],
                      on=['RENTREE', 'UR', 'UI'], how='left')
    return df


def enrich_ing(df, cor_dic):
    df_ing = pd.DataFrame(cor_dic['N_ING'])
    if 'UR' in df.columns and 'UI' in df.columns:
        df = pd.merge(df, df_ing[['RENTREE', 'UR', 'UI', 'ID_PAYSAGE_ING', 'ID_PAYSAGE_ING_CAMPUS']],
                      on=['RENTREE', 'UR', 'UI'], how='left')
    return df


def enrich_lmd(df, cor_dic):
    df_lmd = pd.DataFrame(cor_dic['J_LMDDONT'])
    if 'TYP_DIPL' in df.columns:
        df = pd.merge(df, df_lmd[['TYP_DIPL', 'LMDDONT', 'LMDDONTBIS']],
                      on=['TYP_DIPL'], how='left')
    if 'TYP_DIPINT' in df.columns:
        df_lmd = df_lmd.rename(
            columns={'TYP_DIPL': 'TYP_DIPINT', 'LMDDONT': "LMDDONT_INT", 'LMDDONTBIS': "LMDDONTBIS_INT"})
        df = pd.merge(df, df_lmd[['TYP_DIPINT', 'LMDDONT_INT', 'LMDDONTBIS_INT']],
                      on=['TYP_DIPINT'], how='left')
    return df


def enrich_dndu(df, cor_dic):
    df_dndu = pd.DataFrame(cor_dic['I_DNDU'])
    if 'TYP_DIPL' in df.columns:
        df = pd.merge(df, df_dndu[['TYP_DIPL', 'DNDU']],
                      on=['TYP_DIPL'], how='left')
    if 'TYP_DIPINT' in df.columns:
        df_dndu = df_dndu.rename(columns={'TYP_DIPL': 'TYP_DIPINT', "DNDU": "DNDU_INT"})
        df = pd.merge(df, df_dndu[['TYP_DIPINT', 'DNDU_INT']],
                      on=['TYP_DIPINT'], how='left')
        df.loc[df['TYP_DIPINT'] == "", 'DNDU_INT'] = ""
        df.loc[~df['TYP_DIPINT'].isin(["", "DU"]), "DNDU_INT"] = "DN"
    return df


def enrich_dutbut(df, cor_dic):
    df_dutbut = pd.DataFrame(cor_dic['O_DUTBUT'])
    if 'DIPLOM' in df.columns and 'UI' in df.columns:
        df = pd.merge(df, df_dutbut[['DIPLOM', 'CORRESPONDANCEIUT', 'SPECIUT', 'OPTIUT', 'PARCOURSBUT']],
                      on=['DIPLOM'], how='left')
    return df


def autres_multi(df):
    for c in ['PARCOURSBUT', 'CORRESPONDANCEIUT', 'SPECIUT', 'OPTIUT']:
        df.loc[((df[c] == '') | (pd.isna(df[c])) | (df[c].isnull())), c] = 'AUTRES'
    return df


def effectif_resdip(df):
    df.loc[df['RESDIP'].isin(['O', '1']), 'EFFECTIF_R'] = 1
    df.loc[df['RESDIP'].isin(['N', 'None']), 'EFFECTIF_R'] = 0
    df.loc[df["EFFECTIF_R"].isna(), "EFFECTIF_R"] = 0
    df["EFFECTIF_R"] = df["EFFECTIF_R"].astype(int)
    return df


def effectif_resint(df):
    df.loc[df['RESINT'].isin(['O', '1']), 'EFFECTIF_INT'] = 1
    df.loc[df['RESINT'].isin(['N', 'None']), 'EFFECTIF_INT'] = 0
    df.loc[df["EFFECTIF_INT"].isna(), "EFFECTIF_INT"] = 0
    df["EFFECTIF_INT"] = df["EFFECTIF_INT"].astype(int)
    return df


def LMDdont(df):
    mask_dipl = (df['DIPLOM'].isin(['6001000', '6004000', '8000010']))
    df.loc[mask_dipl, 'LMDDONT'] = 'AUTRES'
    df.loc[mask_dipl, 'LMDDONTBIS'] = 'AUTRES'
    df.loc[df['DNDU'] == 'DU', 'LMDDONTBIS'] = 'DU'

    df.loc[(df['TYP_DIPL'] == 'XA') & (df['PAR_TYPE'] == '0001291'), 'LMDDONTBIS'] = 'LIC_L_LAS'
    df.loc[(df['TYP_DIPL'] == 'XA') & (df['PAR_TYPE'] != '0001291'), 'LMDDONTBIS'] = 'LIC_L_AUT'

    df.loc[(df['TYP_DIPL'] == 'XA') & (df['PAR_TYPE'] != '0001291'), 'LMDDONTBIS'] = 'LIC_L_AUT'

    df.loc[(df['TYP_DIPL'] == '61') & (
        df['DIPLOM'].isin(['9610007', '9610070', '9610171', '9610062', '9610006'])), 'LMDDONTBIS'] = 'REBOND'

    df.loc[(df['TYP_DIPL'] == '63') & (
        df['DIPLOM'].isin(['9630001', '9630067', '9630123', '9630127', '9630133', '9630162', '9630163',
                           '9630166', '9630167', '9630170', '9630190'])), 'LMDDONTBIS'] = 'PAREO'

    df.loc[(df['TYP_DIPL'] == 'XC') & (
        df['DIPLOM'].isin(['2212622',
                           '2212634',
                           '2215257',
                           '2215267',
                           '2215490',
                           '2215491',
                           '2215493',
                           '2215496',
                           '2216535',
                           '2216536',
                           '2216986',
                           '2216988',
                           '2216989',
                           '2217038',
                           '2217039'])), 'LMDDONTBIS'] = 'MAS_AUT'

    if "TYP_DIPINT" in df.columns:
        df.loc[mask_dipl, 'LMDDONT_INT'] = 'AUTRES'
        df.loc[mask_dipl, 'LMDDONTBIS_INT'] = 'AUTRES'
        df.loc[df['DNDU_INT'] == 'DU', 'LMDDONTBIS_INT'] = 'DU'

    df.loc[(df['LMDDONT'] == '') | (pd.isna(df['LMDDONT'])) | (df['LMDDONT'].isnull()), 'LMDDONT'] = 'AUTRES'
    df.loc[
        (df['LMDDONTBIS'] == '') | (pd.isna(df['LMDDONTBIS'])) | (df['LMDDONTBIS'].isnull()), 'LMDDONTBIS'] = 'AUTRES'

    if "TYP_DIPINT" in df.columns:
        df.loc[(df['LMDDONT_INT'] == '') | (pd.isna(df['LMDDONT_INT'])) | (
            df['LMDDONT_INT'].isnull()), 'LMDDONT_INT'] = 'AUTRES'
        df.loc[
            (df['LMDDONTBIS_INT'] == '') | (pd.isna(df['LMDDONTBIS_INT'])) | (
                df['LMDDONTBIS_INT'].isnull()), 'LMDDONTBIS_INT'] = 'AUTRES'

    return df


def niveau_retard_avance(df):
    mask_anbac = ((df['ANBAC'] == '') | (pd.isna(df['ANBAC'])) | (df['ANBAC'].isnull()))

    mask_bac_rgp = ((df['BAC_RGRP'] == '') | (pd.isna(df['BAC_RGRP'])) | (df['BAC_RGRP'].isnull()))
    df.loc[~mask_anbac, 'AGE_BAC'] = df.loc[~mask_anbac, 'ANBAC'] - df.loc[~mask_anbac, 'ANNAIS']
    df.loc[mask_bac_rgp & (df['BAC'] == '0031'), 'BAC_RGRP'] = '7'
    df.loc[mask_bac_rgp & (df['BAC'] == '0032'), 'BAC_RGRP'] = '7'

    mask_age_retard = (df['AGE_BAC'] > 18)
    mask_age_avance = (df['AGE_BAC'] < 18)

    mask_rentree = (df.RENTREE.astype(int) < 2012)
    mask_bac_rgp = (df['BAC_RGRP'].isin(["1", "2", "3", "4", "5"]))
    df.loc[mask_rentree & mask_bac_rgp & mask_age_retard, 'RETARD'] = 'O'
    df.loc[mask_rentree & mask_bac_rgp & ~mask_age_retard, 'RETARD'] = 'N'
    df.loc[mask_rentree & (df['BAC_RGRP'] == '6') & (df['AGE_BAC'] > 19), 'RETARD'] = 'O'
    df.loc[mask_rentree & (df['BAC_RGRP'] == '6') & (df['AGE_BAC'] < 20), 'RETARD'] = 'N'
    df.loc[mask_rentree & mask_bac_rgp & mask_age_avance, 'AVANCE'] = 'O'
    df.loc[mask_rentree & mask_bac_rgp & ~mask_age_avance, 'AVANCE'] = 'N'
    df.loc[mask_rentree & (df['BAC_RGRP'] == '6') & ~mask_age_retard, 'AVANCE'] = 'O'
    df.loc[mask_rentree & (df['BAC_RGRP'] == '6') & mask_age_retard, 'AVANCE'] = 'N'
    df.loc[mask_rentree & (mask_anbac | (df['BAC_RGRP'].isin(["7", "9"]))), 'RETARD'] = "X"
    df.loc[mask_rentree & (mask_anbac | (df['BAC_RGRP'].isin(["7", "9"]))), 'AVANCE'] = "X"

    mask_rentree = (df.RENTREE.astype(int) > 2011)
    mask_bac_rgp = (df['BAC_RGRP'].isin(["1", "2", "3", "4", "5", "6", "A"]))
    df.loc[mask_rentree & mask_bac_rgp & mask_age_retard, 'RETARD'] = 'O'
    df.loc[mask_rentree & mask_bac_rgp & ~mask_age_retard, 'RETARD'] = 'N'
    df.loc[mask_rentree & mask_bac_rgp & mask_age_avance, 'AVANCE'] = 'O'
    df.loc[mask_rentree & mask_bac_rgp & ~mask_age_avance, 'AVANCE'] = 'N'

    mask_anbac = ((df['ANBAC'] == '') | (pd.isna(df['ANBAC'])) | (df['ANBAC'].isnull()))
    df.loc[mask_rentree & (mask_anbac | ~mask_bac_rgp), 'RETARD'] = "X"
    df.loc[mask_rentree & (mask_anbac | ~mask_bac_rgp), 'AVANCE'] = "X"

    df.loc[(df['BAC_RGRP'] == '9') & mask_anbac, 'BAC_RGRP'] = '7'
    return df


def proximite_correctifs(df):
    mask_depbac_rgrp = ((df['DEPBAC'] == '000') | (df['DEPBAC'] == '') | (pd.isna(df['DEPBAC'])) | (
        df['DEPBAC'].isnull())) & (df['BAC_RGRP'] != '7')
    df.loc[(df['BAC_RGRP'] == '9') & (
            (df['PROXBAC'] == '') | (pd.isna(df['PROXBAC'])) | (df['PROXBAC'].isnull())), 'PROXBAC'] = "5 - NR"
    df.loc[df['BAC_RGRP'] == '7', 'PROXBAC'] = '9 - non-bachelier'
    df.loc[mask_depbac_rgrp, 'PROXBAC'] = "5 - NR"
    df.loc[df['BAC_RGRP'] == '7', 'PROXREGBAC'] = '9 - non-bachelier'
    df.loc[(df['BAC_RGRP'] == '9') & ((df['PROXREGBAC'] == '') | (pd.isna(df['PROXREGBAC'])) | (
        df['PROXREGBAC'].isnull())), 'PROXREGBAC'] = "3 - NR"
    df.loc[mask_depbac_rgrp, 'PROXREGBAC'] = "3 - NR"

    df.loc[(df['BAC_RGRP'] == '7'), 'OUTREMER'] = 'non-bachelier'
    return df


def deptoreg(df, cor_dic):
    deptoreg = pd.DataFrame(cor_dic['DEPTOREG'])
    df = pd.merge(df, deptoreg, how='left', left_on='DEPRESPA', right_on='IN').drop(columns={'IN'}).rename(
        columns={'OUT': 'REGRESPA'})
    deptoregnew = pd.DataFrame(cor_dic['DEPTOREGNEW'])
    df = pd.merge(df, deptoregnew, how='left', left_on='DEPRESPA', right_on='IN').drop(columns={'IN'}).rename(
        columns={'OUT': 'REGRESPA16'})
    return df


def corrige2018_2024(df):
    mask_bac = df['BAC'].isin(["0031", "0001", "0002"])
    df.loc[mask_bac, 'NATION_BAC'] = "E"
    df.loc[~mask_bac, 'NATION_BAC'] = "F"
    df.loc[(df['NATION_BAC'] == 'E') & (df['FR_ETR'] == '2'), 'NATION_VRAI'] = "E"
    df.loc[(df['NATION_BAC'] != 'E') | (df['FR_ETR'] != '2'), 'NATION_VRAI'] = "F"

    df.loc[(df['NATION'] != '100') & ((df['ACABAC'] == '00') | (df['BAC'] == '0031')), 'MOBINTERN'] = "M"
    df.loc[(df['NATION'] == '100') | ((df['ACABAC'] != '00') & (df['BAC'] != '0031')), 'MOBINTERN'] = "X"

    df.loc[df['BAC_RGRP'].isin(["1", "2", "3"]), 'BAC_RGRP'] = 'A'

    df.loc[((df['DNDU'] == '') | (pd.isna(df['DNDU'])) | (df['DNDU'].isnull())), 'DNDU'] = 'DN'
    df.loc[(df['TYP_DIPL'] == 'XA') & (df['CURSUS_LMD'] != 'L'), 'CURSUS_LMD'] = 'L'
    df.loc[(df['TYP_DIPL'] == 'DU') & (df['CURSUS_LMD'] == 'D'), 'CURSUS_LMD'] = 'M'

    if "TYP_DIPINT" in df.columns:
        df.loc[(df['TYP_DIPINT'] == 'XA') & (df['CURSUS_LMD_INT'] != 'L'), 'CURSUS_LMD_INT'] = 'L'
        df.loc[(df['TYP_DIPINT'] == 'DU') & (df['CURSUS_LMD_INT'] == 'D'), 'CURSUS_LMD_INT'] = 'M'
    return df


def cal_var(df, stage: str):
    model_vars = ['ANNEE', 'RENTREE', 'SOURCE', 'ID_PAYSAGE', 'ID_PAYSAGE_EPE', 'ID_PAYSAGE_EPE_ETAB_COMPOS',
                  'ID_PAYSAGE_FORMENS',
                  'UI', 'UR', 'ETABLI', 'ETABLI_ORI_UAI', 'ETABLI_DIFFUSION', 'ETABLIESPE', 'PAR_TYPE', 'NUMED',
                  'OPPOS', 'COMPOS',
                  'INSPR', 'REGIME', 'ANNAIS', 'SEXE', 'BAC', 'ANBAC', 'SITUPRE', 'DIPDER', 'CURPAR', 'NATION',
                  'PARIPA', 'CP_ETU',
                  'PAYPAR', 'CP_PAR', 'PCSPAR', 'ECHANG', 'DIPLOM', 'NIVEAU', 'SPECIA', 'SPECIB', 'SPECIC', 'ANSUP',
                  'LCOMETU', 'LCOMREF',
                  'AMENA', 'TYPREPA', 'PCSPAR2', 'COMREF', 'COMETU', 'FR_ETR', 'FR_ETR_R', 'FR_ETR_D', 'ACAETA',
                  'DEPETA',
                  'COMETA', 'BAC_RGRP', 'TYP_DIPL', 'TYP_DIPINT', 'SECTDIS', 'DISCIPLI', 'NATURE', 'CYCLE', 'NIVEAU_D',
                  'NIVEAU_F',
                  'AGE', 'DEPRESPA',
                  'ACARESPA', 'COMINS', 'DEGETU', 'NBACH', 'NET', 'EFFECTIF_R', 'EFFECTIF_INT', 'GROUPE', 'CURSUS_LMD',
                  'VOIE', 'NATRG',
                  'ACABAC',
                  'COMETAB', 'COMUI', 'DEPBAC', 'DEPINS', 'UUCRINS', 'UUCRETAB', 'PROXIMITE', 'PROXREG', 'PROXBAC',
                  'PROXREGBAC', 'OUTREMER',
                  'CONTINENT', 'UE_28', 'UE_27', 'UE_EURO', 'OCDE_MEMBRES', 'OCDE_OBS', 'BOLOGNE', 'BRICS',
                  'ID_PAYSAGE_ED', 'ID_PAYSAGE_IUT',
                  'ID_PAYSAGE_IUT_CAMPUS', 'ID_PAYSAGE_IUT_POLE', 'ID_PAYSAGE_ING', 'ID_PAYSAGE_ING_CAMPUS', 'LMDDONT',
                  'LMDDONTBIS', 'DNDU',
                  'CORRESPONDANCEIUT', 'SPECIUT', 'OPTIUT', 'PARCOURSBUT',
                  'NBACH_CESURE', 'CONV', 'EFF_DIE',
                  'EFF_SS_CPGE', 'NBACH_SS_CPGE', 'EFFT_SS_CPGE', 'EFFS_SS_CPGE', 'GDDISC', 'NIVEAUBIS', 'AGE_BAC',
                  'RETARD',
                  'AVANCE', 'REGRESPA', 'REGRESPA16', 'NATION_BAC', 'NATION_VRAI', 'MOBINTERN']

    df_cols = df.columns

    missing = pd.DataFrame(columns=list(set(model_vars) - set(df_cols)))
    surplus = list(set(df_cols) - set(model_vars))

    if stage == 'end':
        print(f"adding missing model vars into df_cleaned -> {list(missing.columns)}", flush=True)
        df = pd.concat([missing, df], ignore_index=True)
        print(f"removing surplus vars in df_cleaned and not in model {surplus}", flush=True)
        df = df.drop(columns=surplus)
    elif stage == 'init':
        print(f"removing surplus vars in df_cleaned and not in model {surplus}", flush=True)
        df = df.drop(columns=surplus)

    return df


def init_cal_var(df):
    model_vars = ['ANNEE', 'RENTREE', 'SOURCE', 'ID_PAYSAGE', 'ID_PAYSAGE_EPE', 'ID_PAYSAGE_EPE_ETAB_COMPOS',
                  'ID_PAYSAGE_FORMENS',
                  'UI', 'UR', 'ETABLI', 'ETABLI_ORI_UAI', 'ETABLI_DIFFUSION', 'ETABLIESPE', 'PAR_TYPE', 'NUMED',
                  'OPPOS', 'COMPOS',
                  'INSPR', 'REGIME', 'ANNAIS', 'SEXE', 'BAC', 'ANBAC', 'SITUPRE', 'DIPDER', 'CURPAR', 'NATION',
                  'PARIPA', 'CP_ETU',
                  'PAYPAR', 'CP_PAR', 'PCSPAR', 'ECHANG', 'DIPLOM', 'NIVEAU', 'SPECIA', 'SPECIB', 'SPECIC', 'ANSUP',
                  'LCOMETU', 'LCOMREF',
                  'AMENA', 'TYPREPA', 'PCSPAR2', 'COMREF', 'COMETU', 'FR_ETR', 'FR_ETR_R', 'FR_ETR_D', 'ACAETA',
                  'DEPETA',
                  'COMETA', 'BAC_RGRP', 'TYP_DIPL', 'TYP_DIPINT', 'SECTDIS', 'DISCIPLI', 'NATURE', 'CYCLE', 'NIVEAU_D',
                  'NIVEAU_F',
                  'AGE', 'DEPRESPA',
                  'ACARESPA', 'COMINS', 'DEGETU', 'NBACH', 'NET', 'EFFECTIF_R', "EFFECTIF_INT", 'GROUPE', 'CURSUS_LMD',
                  'VOIE', 'NATRG',
                  'ACABAC',
                  'COMUI', 'DEPBAC', 'DEPINS', 'UUCRINS', 'UUCRETAB', 'PROXIMITE', 'PROXREG', 'PROXBAC', 'PROXREGBAC',
                  'OUTREMER',
                  'CONTINENT', 'UE_28', 'UE_27', 'UE_EURO', 'OCDE_MEMBRES', 'OCDE_OBS', 'BOLOGNE', 'BRICS',
                  'ID_PAYSAGE_ED', 'ID_PAYSAGE_IUT',
                  'ID_PAYSAGE_IUT_CAMPUS', 'ID_PAYSAGE_IUT_POLE', 'ID_PAYSAGE_ING', 'ID_PAYSAGE_ING_CAMPUS', 'LMDDONT',
                  'LMDDONTBIS', 'DNDU',
                  'CORRESPONDANCEIUT', 'SPECIUT', 'OPTIUT', 'PARCOURSBUT', 'NBACH_CESURE', 'CONV',
                  'EFF_SS_CPGE', 'NBACH_SS_CPGE', 'EFFT_SS_CPGE', 'EFFS_SS_CPGE', 'GDDISC', 'NIVEAUBIS', 'AGE_BAC',
                  'RETARD',
                  'AVANCE', 'REGRESPA', 'REGRESPA16', 'NATION_BAC', 'NATION_VRAI', 'MOBINTERN', 'FLAG_SUP', 'FLAG_EPE']

    surplus = list(set(df.columns) - set(model_vars))
    print(f"removing surplus vars in df_source and not in first model {surplus}")
    df = df.drop(columns=surplus)
    return df
