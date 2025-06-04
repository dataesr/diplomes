import numpy as np
import pandas as pd
import os
import time

from application.server.main.logger import get_logger
from diplomes import dtypes_inputs as typesd
from diplomes.u1_google_sheets import get_all_correctifs
from utils import swift

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")

D = ['UO_LIB', 'ID_PAYSAGE']
G = ['RENTREE', 'ANNEE_UNIVERSITAIRE']
C = ['UO_LIB', 'TYPE', 'TYPOLOGIE_D_UNIVERSITES_ET_ASSIMILES', 'ANCIENS_CODES_UAI', 'IDENTIFIANT_WIKIDATA',
     'IDENTIFIANT_ROR', 'OPERATEUR_LOLF_150', 'ID_PAYSAGE_ACTUEL', 'ID_PAYSAGE']
H = ["COM_NOM", "UUCR_ID", "UUCR_NOM", "DEP_ID", "DEP_NOM", "ACA_ID", "ACA_NOM", "REG_ID", "REG_NOM", "COM_CODE"]

dict_o = {'ID_PAYSAGE_EPE_ETAB_COMPOS': 'ETABLISSEMENT_COMPOS_ID_PAYSAGE', 'ID_PAYSAGE': 'ETABLISSEMENT_ID_PAYSAGE',
          'ID_PAYSAGE_FORMENS': 'FORM_ENS_ID_PAYSAGE', 'COM_ETAB': 'ETABLISSEMENT_CODE_COMMUNE',
          'COM_INS': 'IMPLANTATION_CODE_COMMUNE', 'ETABLI_ORI_UAI': 'ETABLISSEMENT_ID_UAI_SOURCE',
          'ID_PAYSAGE_EPE_ETAB_COMPOS': 'ETABLISSEMENT_COMPOS_ID_PAYSAGE'}
dict_c = {'UO_LIB': 'ETABLISSEMENT_LIB', 'TYPE': 'ETABLISSEMENT_TYPE',
          'TYPOLOGIE_D_UNIVERSITES_ET_ASSIMILES': 'ETABLISSEMENT_TYPOLOGIE',
          'ANCIENS_CODES_UAI': 'ETABLISSEMENT_ID_UAI', 'IDENTIFIANT_WIKIDATA': 'ETABLISSEMENT_ID_WIKIDATA',
          'IDENTIFIANT_ROR': 'ETABLISSEMENT_ID_ROR', 'ID_PAYSAGE_ACTUEL': 'ETABLISSEMENT_ID_PAYSAGE_ACTUEL'}
dict_e = {'UO_LIB': 'ETABLISSEMENT_COMPOS_LIB'}
dict_f = {'UO_LIB': 'FORM_ENS_LIB'}
dict_h = {'COM_NOM': 'ETABLISSEMENT_COMMUNE', 'UUCR_ID': 'ETABLISSEMENT_ID_UUCR', 'UUCR_NOM': 'ETABLISSEMENT_UUCR',
          'DEP_ID': 'ETABLISSEMENT_ID_DEPARTEMENT', 'DEP_NOM': 'ETABLISSEMENT_DEPARTEMENT',
          'ACA_ID': 'ETABLISSEMENT_ID_ACADEMIE', 'ACA_NOM': 'ETABLISSEMENT_ACADEMIE',
          'REG_ID': 'ETABLISSEMENT_ID_REGION', 'REG_NOM': 'ETABLISSEMENT_REGION'}
dict_i = {'COM_NOM': 'IMPLANTATION_COMMUNE', 'UUCR_ID': 'IMPLANTATION_ID_UUCR', 'UUCR_NOM': 'IMPLANTATION_UUCR',
          'DEP_ID': 'IMPLANTATION_ID_DEPARTEMENT', 'DEP_NOM': 'IMPLANTATION_DEPARTEMENT',
          'ACA_ID': 'IMPLANTATION_ID_ACADEMIE', 'ACA_NOM': 'IMPLANTATION_ACADEMIE', 'REG_ID': 'IMPLANTATION_ID_REGION',
          'REG_NOM': 'IMPLANTATION_REGION'}
dict_d = {'UO_LIB': 'ETABLISSEMENT_ACTUEL_LIB'}
CORRECTIFS_DICT = get_all_correctifs("google")
C_ETABLISSEMENT = pd.DataFrame(CORRECTIFS_DICT['C_ETABLISSEMENTS'])


def get_sources(annee):
    assert (annee >= 2015)
    sources = ['result', 'ens', 'inge', 'priv']
    if annee > 2017:
        sources.append('mana')
    if annee > 2018:
        sources.append('enq')
        sources.append('culture')
    return sources


def read_diplome(source, year):
    os.chdir(DATA_PATH)
    # an = str(year)[2:4]
    logger.debug(f'reading parquet file diplomes for {source} {year} ...')
    if source == "result":
        filename = f'corrected_{source}{year}_ssa.parquet'
    elif source == "enq":
        filename = f'corrected_dip{source}26bis{year}_ssa.parquet'
    else:
        filename = f'corrected_dip{source}{year}_ssa.parquet'

    df = pd.read_parquet(f'{DATA_PATH}parquet/{filename}')

    return df


def sise(df):
    df = df.loc[df["EFFECTIF_TOT"] > 0]
    op150 = C_ETABLISSEMENT[["ID_PAYSAGE", "OPERATEUR_LOLF_150"]]
    df = pd.merge(df, op150, on="ID_PAYSAGE", how="left")
    df = df.loc[df["OPERATEUR_LOLF_150"] == "O"]
    df = df.drop(columns="OPERATEUR_LOLF_150")
    if "DNDU_INT" in df.columns:
        tmp = df.groupby(
            ['RENTREE', 'SESSION', 'ID_PAYSAGE', 'ID_PAYSAGE_EPE_ETAB_COMPOS', 'ID_PAYSAGE_FORMENS', 'ID_PAYSAGE_ED',
             'ID_PAYSAGE_ING', 'ID_PAYSAGE_ING_CAMPUS', 'ID_PAYSAGE_IUT', 'ID_PAYSAGE_IUT_CAMPUS',
             'ID_PAYSAGE_IUT_POLE',
             'ETABLI_ORI_UAI', 'COMUI', 'COMETAB', 'SEXE', 'BAC_RGRP', 'RETARD', 'AVANCE', 'PROXBAC', 'PROXREGBAC',
             'NATION_VRAI', 'MOBINTERN', 'DNDU', 'DNDU_INT', 'CURSUS_LMD', 'CURSUS_LMDR', 'CURSUS_LMD_INT',
             'LMDDONTBIS',
             "LMDDONTBIS_INT", 'RESDIP', 'RESINT',
             'SECTDIS', 'SECTINT', 'SPECIUT', 'OPTIUT', 'PARCOURSBUT', 'UE_27', 'OCDE_MEMBRES', 'BOLOGNE',
             'BRICS', 'NIVEAU'], as_index=False, dropna=False).agg(
            {'EFFECTIF_INT': 'sum', 'EFFECTIF_R': 'sum', 'EFFECTIF_TOT': 'sum', 'UE_27': 'sum', 'OCDE_MEMBRES': 'sum',
             'BOLOGNE': 'sum',
             'BRICS': 'sum'})
    else:
        tmp = df.groupby(
            ['RENTREE', 'SESSION', 'ID_PAYSAGE', 'ID_PAYSAGE_EPE_ETAB_COMPOS', 'ID_PAYSAGE_FORMENS', 'ID_PAYSAGE_ED',
             'ID_PAYSAGE_ING', 'ID_PAYSAGE_ING_CAMPUS', 'ID_PAYSAGE_IUT', 'ID_PAYSAGE_IUT_CAMPUS',
             'ID_PAYSAGE_IUT_POLE',
             'ETABLI_ORI_UAI', 'COMUI', 'COMETAB', 'SEXE', 'BAC_RGRP', 'RETARD', 'AVANCE', 'PROXBAC', 'PROXREGBAC',
             'NATION_VRAI', 'MOBINTERN', 'DNDU', 'CURSUS_LMD', 'CURSUS_LMDR', 'CURSUS_LMD_INT',
             'LMDDONTBIS',
             "LMDDONTBIS_INT", 'RESDIP', 'RESINT',
             'SECTDIS', 'SECTINT', 'SPECIUT', 'OPTIUT', 'PARCOURSBUT', 'UE_27', 'OCDE_MEMBRES', 'BOLOGNE',
             'BRICS', 'NIVEAU'], as_index=False, dropna=False).agg(
            {'EFFECTIF_INT': 'sum', 'EFFECTIF_R': 'sum', 'EFFECTIF_TOT': 'sum', 'UE_27': 'sum', 'OCDE_MEMBRES': 'sum',
             'BOLOGNE': 'sum',
             'BRICS': 'sum'})
    tmp.loc[tmp['SEXE'] == "1", 'SEXE'] = "M"
    tmp.loc[tmp['SEXE'] == "2", 'SEXE'] = "F"
    tmp["OBTENU_DIPLOME"] = tmp["RESDIP"].apply(lambda x: "Oui" if x == "O" else "Non")
    tmp["OBTENU_DIPINT"] = tmp["RESINT"].apply(lambda x: "Oui" if x == "O" else "Non")
    tmp['AVANCE_RETARD'] = tmp.loc[:, 'AVANCE'] + tmp.loc[:, 'RETARD']
    tmp['ATTRAC_INTERN_UE_27'] = tmp.loc[:, 'UE_27']
    tmp['ATTRAC_INTERN_OCDE_MEMBRES'] = tmp.loc[:, 'OCDE_MEMBRES']
    tmp['ATTRAC_INTERN_BOLOGNE'] = tmp.loc[:, 'BOLOGNE']
    tmp['ATTRAC_INTERN_BRICS'] = tmp.loc[:, 'BRICS']
    tmp['ATTRAC_INTERN'] = tmp.loc[:, 'NATION_VRAI']
    tmp['MOBILITE_INTERN'] = tmp.loc[:, 'MOBINTERN']
    tmp['COM_INS'] = tmp.loc[:, 'COMUI']
    tmp['COM_ETAB'] = tmp.loc[:, 'COMETAB']
    tmp['EFFECTIF_TOTAL'] = tmp.loc[:, 'EFFECTIF_TOT']

    tmp = tmp.drop(
        columns=['AVANCE', 'RETARD', 'NATION_VRAI', 'MOBINTERN', 'COMUI', 'COMETAB', 'EFFECTIF_TOT'])

    if "DNDU_INT" in df.columns:
        tmp = tmp.groupby(
            ['RENTREE', 'SESSION', 'ID_PAYSAGE', 'ID_PAYSAGE_EPE_ETAB_COMPOS', 'ID_PAYSAGE_FORMENS', 'ID_PAYSAGE_ED',
             'ID_PAYSAGE_ING', 'ID_PAYSAGE_ING_CAMPUS', 'ID_PAYSAGE_IUT', 'ID_PAYSAGE_IUT_CAMPUS',
             'ID_PAYSAGE_IUT_POLE',
             'ETABLI_ORI_UAI', 'COM_INS', 'COM_ETAB', 'SEXE', 'BAC_RGRP', 'AVANCE_RETARD', 'PROXBAC', 'PROXREGBAC',
             'ATTRAC_INTERN', 'MOBILITE_INTERN', 'DNDU', 'DNDU_INT', 'CURSUS_LMD', 'CURSUS_LMDR', 'CURSUS_LMD_INT',
             'LMDDONTBIS', "LMDDONTBIS_INT", 'NIVEAU', 'SECTDIS', 'SECTINT', 'SPECIUT',
             'OPTIUT', 'PARCOURSBUT', "OBTENU_DIPLOME", "OBTENU_DIPINT"], as_index=False, dropna=False).agg(
            {'EFFECTIF_INT': 'sum', 'EFFECTIF_R': 'sum', 'EFFECTIF_TOTAL': 'sum', 'ATTRAC_INTERN_UE_27': 'sum',
             'ATTRAC_INTERN_OCDE_MEMBRES': 'sum', 'ATTRAC_INTERN_BOLOGNE': 'sum',
             'ATTRAC_INTERN_BRICS': 'sum'})
    else:
        tmp = tmp.groupby(
            ['RENTREE', 'SESSION', 'ID_PAYSAGE', 'ID_PAYSAGE_EPE_ETAB_COMPOS', 'ID_PAYSAGE_FORMENS', 'ID_PAYSAGE_ED',
             'ID_PAYSAGE_ING', 'ID_PAYSAGE_ING_CAMPUS', 'ID_PAYSAGE_IUT', 'ID_PAYSAGE_IUT_CAMPUS',
             'ID_PAYSAGE_IUT_POLE',
             'ETABLI_ORI_UAI', 'COM_INS', 'COM_ETAB', 'SEXE', 'BAC_RGRP', 'AVANCE_RETARD', 'PROXBAC', 'PROXREGBAC',
             'ATTRAC_INTERN', 'MOBILITE_INTERN', 'DNDU', 'CURSUS_LMD', 'CURSUS_LMDR', 'CURSUS_LMD_INT',
             'LMDDONTBIS', "LMDDONTBIS_INT", 'NIVEAU', 'SECTDIS', 'SECTINT', 'SPECIUT',
             'OPTIUT', 'PARCOURSBUT', "OBTENU_DIPLOME", "OBTENU_DIPINT"], as_index=False, dropna=False).agg(
            {'EFFECTIF_INT': 'sum', 'EFFECTIF_R': 'sum', 'EFFECTIF_TOTAL': 'sum', 'ATTRAC_INTERN_UE_27': 'sum',
             'ATTRAC_INTERN_OCDE_MEMBRES': 'sum', 'ATTRAC_INTERN_BOLOGNE': 'sum',
             'ATTRAC_INTERN_BRICS': 'sum'})
    return tmp


def opendata19(df):
    E_FORM_ENS = pd.DataFrame(CORRECTIFS_DICT['E_FORM_ENS'])
    F_RENTREES = pd.DataFrame(CORRECTIFS_DICT['F_RENTREES'])
    communes = pd.DataFrame(CORRECTIFS_DICT['LES_COMMUNES'])

    O = ['RENTREE', 'SESSION', 'ID_PAYSAGE', 'ID_PAYSAGE_FORMENS', 'COM_ETAB', 'COM_INS', 'ETABLI_ORI_UAI',
         'ID_PAYSAGE_EPE_ETAB_COMPOS', 'SEXE', 'AVANCE_RETARD', 'PROXBAC', 'BAC_RGRP', 'PROXREGBAC',
         'ATTRAC_INTERN', 'MOBILITE_INTERN', 'DNDU', 'CURSUS_LMD', 'CURSUS_LMD_INT', 'LMDDONTBIS',
         'LMDDONTBIS_INT', 'NIVEAU', 'SECTDIS', 'SECTINT', 'SPECIUT', 'ID_PAYSAGE_ED', 'ID_PAYSAGE_ING',
         'ID_PAYSAGE_ING_CAMPUS', 'ID_PAYSAGE_IUT', 'ID_PAYSAGE_IUT_CAMPUS', 'ID_PAYSAGE_IUT_POLE', 'OPTIUT',
         'PARCOURSBUT', 'OBTENU_DIPINT', 'OBTENU_DIPLOME', 'ATTRAC_INTERN_BOLOGNE', 'ATTRAC_INTERN_BRICS',
         'ATTRAC_INTERN_OCDE_MEMBRES', 'ATTRAC_INTERN_UE_27', 'EFFECTIF_R', 'EFFECTIF_INT', 'EFFECTIF_TOTAL']

    if "DNDU_INT" in df.columns:
        O.append("DNDU_INT")

    temp_1 = pd.merge(df[O].rename(columns=dict_o), C_ETABLISSEMENT[C].rename(columns=dict_c), how='left',
                      left_on='ETABLISSEMENT_ID_PAYSAGE', right_on='ID_PAYSAGE')
    del temp_1['ID_PAYSAGE']
    temp_2 = pd.merge(temp_1, C_ETABLISSEMENT[D].rename(columns=dict_d), how='left',
                      left_on='ETABLISSEMENT_ID_PAYSAGE_ACTUEL', right_on='ID_PAYSAGE')
    del temp_2['ID_PAYSAGE']
    temp_3 = pd.merge(temp_2, C_ETABLISSEMENT[D].rename(columns=dict_e), how='left',
                      left_on='ETABLISSEMENT_COMPOS_ID_PAYSAGE', right_on='ID_PAYSAGE')
    del temp_3['ID_PAYSAGE']
    temp_4 = pd.merge(temp_3, E_FORM_ENS[D].rename(columns=dict_f), how='left', left_on='FORM_ENS_ID_PAYSAGE',
                      right_on='ID_PAYSAGE')
    temp_5 = pd.merge(temp_4, F_RENTREES[G], how='left', on='RENTREE')
    temp_6 = pd.merge(temp_5, communes[H].rename(columns=dict_h), how='left', left_on='ETABLISSEMENT_CODE_COMMUNE',
                      right_on='COM_CODE')
    del temp_6['COM_CODE']
    temp = pd.merge(temp_6, communes[H].rename(columns=dict_i), how='left', left_on='IMPLANTATION_CODE_COMMUNE',
                    right_on='COM_CODE')

    temp['ETABLISSEMENT_LOCALISATION'] = temp.loc[:, 'ETABLISSEMENT_REGION']
    temp['IMPLANTATION_LOCALISATION'] = temp.loc[:, 'IMPLANTATION_REGION']

    temp.loc[temp['ETABLISSEMENT_REGION'] != temp['ETABLISSEMENT_ACADEMIE'], 'ETABLISSEMENT_LOCALISATION'] = temp[
                                                                                                                 'ETABLISSEMENT_LOCALISATION'] + ">" + \
                                                                                                             temp[
                                                                                                                 'ETABLISSEMENT_ACADEMIE']
    temp.loc[temp['IMPLANTATION_REGION'] != temp['IMPLANTATION_ACADEMIE'], 'IMPLANTATION_LOCALISATION'] = temp[
                                                                                                              'IMPLANTATION_LOCALISATION'] + ">" + \
                                                                                                          temp[
                                                                                                              'IMPLANTATION_ACADEMIE']
    temp.loc[temp['ETABLISSEMENT_ACADEMIE'] != temp['ETABLISSEMENT_DEPARTEMENT'], 'ETABLISSEMENT_LOCALISATION'] = temp[
                                                                                                                      'ETABLISSEMENT_LOCALISATION'] + ">" + \
                                                                                                                  temp[
                                                                                                                      'ETABLISSEMENT_DEPARTEMENT']
    temp.loc[temp['IMPLANTATION_ACADEMIE'] != temp['IMPLANTATION_DEPARTEMENT'], 'IMPLANTATION_LOCALISATION'] = temp[
                                                                                                                   'IMPLANTATION_LOCALISATION'] + ">" + \
                                                                                                               temp[
                                                                                                                   'IMPLANTATION_DEPARTEMENT']
    temp.loc[temp['ETABLISSEMENT_DEPARTEMENT'] != temp['ETABLISSEMENT_UUCR'], 'ETABLISSEMENT_LOCALISATION'] = temp[
                                                                                                                  'ETABLISSEMENT_LOCALISATION'] + ">" + \
                                                                                                              temp[
                                                                                                                  'ETABLISSEMENT_UUCR']
    temp.loc[temp['IMPLANTATION_DEPARTEMENT'] != temp['IMPLANTATION_UUCR'], 'IMPLANTATION_LOCALISATION'] = temp[
                                                                                                               'IMPLANTATION_LOCALISATION'] + ">" + \
                                                                                                           temp[
                                                                                                               'IMPLANTATION_UUCR']
    temp.loc[temp['ETABLISSEMENT_UUCR'] != temp['ETABLISSEMENT_COMMUNE'], 'ETABLISSEMENT_LOCALISATION'] = temp[
                                                                                                              'ETABLISSEMENT_LOCALISATION'] + ">" + \
                                                                                                          temp[
                                                                                                              'ETABLISSEMENT_COMMUNE']
    temp.loc[temp['IMPLANTATION_UUCR'] != temp['IMPLANTATION_COMMUNE'], 'IMPLANTATION_LOCALISATION'] = temp[
                                                                                                           'IMPLANTATION_LOCALISATION'] + ">" + \
                                                                                                       temp[
                                                                                                           'IMPLANTATION_COMMUNE']

    # import des "formats" SEXE,BAC_RGRP...

    SEXE = pd.DataFrame(CORRECTIFS_DICT['SEXE'])
    BAC_RGRP = pd.DataFrame(CORRECTIFS_DICT['BAC_RGRP'])
    AVANCE_RETARD = pd.DataFrame(CORRECTIFS_DICT['AVANCE_RETARD'])[['AVANCE_RETARD', 'BAC_AGE', 'BAC_AGE_LIB']]
    PROXBAC = pd.DataFrame(CORRECTIFS_DICT['PROXBAC'])
    PROXREGBAC = pd.DataFrame(CORRECTIFS_DICT['PROXREGBAC'])
    ATTRAC_INTERN = pd.DataFrame(CORRECTIFS_DICT['ATTRAC_INTERN'])
    MOBILITE_INTERN = pd.DataFrame(CORRECTIFS_DICT['MOBILITE_INTERN'])
    DNDU = pd.DataFrame(CORRECTIFS_DICT['DNDU'])[['DNDU', 'DN_DE', 'DN_DE_LIB']]
    DNDU_INT = DNDU.copy()
    DNDU_INT = DNDU_INT.rename(columns={"DNDU": "DNDU_INT", "DN_DE_LIB": "DN_DE_LIB_INT", "DN_DE": "DN_DE_INT"})
    CURSUS_LMD = pd.DataFrame(CORRECTIFS_DICT['CURSUS_LMD'])
    CURSUS_LMD_INT = CURSUS_LMD.copy()
    CURSUS_LMD_INT = CURSUS_LMD_INT.rename(
        columns={"CURSUS_LMD": "CURSUS_LMD_INT", "CURSUS_LMD_LIB": "CURSUS_LMD_LIB_INT"})
    LMDDONTBIS = pd.DataFrame(CORRECTIFS_DICT['LMDDONTBIS'])
    LMDDONTBIS_INT = LMDDONTBIS.copy()
    LMDDONTBIS_INT = LMDDONTBIS_INT.rename(
        columns={"LMDDONTBIS": "LMDDONTBIS_INT", "DIPLOME": "DIPLOME_INT", "DIPLOME_RGP": "DIPLOME_RGP_INT",
                 "DIPLOME_LIB": "DIPLOME_LIB_INT"})
    NIVEAU = pd.DataFrame(CORRECTIFS_DICT['NIVEAU'])
    SECTDIS = pd.DataFrame(CORRECTIFS_DICT['SECTDIS'])[
        ['SECTDIS', 'GD_DISCIPLINE', 'GD_DISCIPLINE_LIB', 'DISCIPLINE', 'DISCIPLINE_LIB', 'SECT_DISCIPLINAIRE',
         'SECT_DISCIPLINAIRE_LIB', 'DISCIPLINES_SELECTION']]
    SECTINT = SECTDIS.copy()
    SECTINT = SECTINT.rename(
        columns={'SECTDIS': 'SECTINT', 'GD_DISCIPLINE': 'GD_DISCIPLINE_INT',
                 'GD_DISCIPLINE_LIB': "GD_DISCIPLINE_LIB_INT",
                 'DISCIPLINE': "DISCIPLINE_INT", 'DISCIPLINE_LIB': "DISCIPLINE_LIB_INT",
                 'SECT_DISCIPLINAIRE': "SECT_DISCIPLINAIRE_INT",
                 'SECT_DISCIPLINAIRE_LIB': "SECT_DISCIPLINAIRE_LIB_INT",
                 'DISCIPLINES_SELECTION': "DISCIPLINES_SELECTION_INT"})
    SPECIUT = pd.DataFrame(CORRECTIFS_DICT['SPECIUT'])[
        ['SPECIUT', 'SPEC_IUT_RGP_LIB', 'SPEC_IUT', 'SPEC_IUT_LIB', 'IUT_ID_PAYSAGE', 'CORRESPONDANCE_IUT']]

    A_temp = pd.merge(temp, SEXE, how='left', on='SEXE')
    A_temp.loc[A_temp["SEXE_LIB"].isna(), "SEXE_LIB"] = "Inconnu"
    B_temp = pd.merge(A_temp, BAC_RGRP, how='left', on='BAC_RGRP')
    C_temp = pd.merge(B_temp, AVANCE_RETARD, how='left', on='AVANCE_RETARD')
    D_temp = pd.merge(C_temp, PROXBAC, how='left', on='PROXBAC')
    E_temp = pd.merge(D_temp, PROXREGBAC, how='left', on='PROXREGBAC')
    F_temp = pd.merge(E_temp, ATTRAC_INTERN, how='left', on='ATTRAC_INTERN')
    G_temp = pd.merge(F_temp, MOBILITE_INTERN, how='left', on='MOBILITE_INTERN')
    H_temp = pd.merge(G_temp, DNDU, how='left', on='DNDU')
    if "DNDU_INT" in H_temp.columns:
        H_int_temp = pd.merge(H_temp, DNDU_INT, how='left', on='DNDU_INT')
    else:
        H_int_temp = H_temp.copy()
    I_temp = pd.merge(H_int_temp, CURSUS_LMD, how='left', on='CURSUS_LMD')
    I_int_temp = pd.merge(I_temp, CURSUS_LMD_INT, how='left', on='CURSUS_LMD_INT')
    J_temp = pd.merge(I_int_temp, LMDDONTBIS, how='left', on='LMDDONTBIS')
    J_int_temp = pd.merge(J_temp, LMDDONTBIS_INT, how='left', on='LMDDONTBIS_INT')
    K_temp = pd.merge(J_int_temp, NIVEAU, how='left', on='NIVEAU')
    L_temp = pd.merge(K_temp, SECTDIS, how='left', on='SECTDIS')
    L_int_temp = pd.merge(L_temp, SECTINT, how='left', on='SECTINT')
    M_temp = pd.merge(L_int_temp, SPECIUT, how='left', on='SPECIUT')

    OD = M_temp.groupby(['ANNEE_UNIVERSITAIRE', 'ATTRAC_INTERN', 'ATTRAC_INTERN_LIB', 'BAC',
                         'BAC_AGE_LIB', 'BAC_LIB', 'BAC_RGP', 'CURSUS_LMD', 'CURSUS_LMD_LIB', "CURSUS_LMD_INT",
                         "CURSUS_LMD_LIB_INT", 'DIPLOME', 'DIPLOME_INT',
                         'DIPLOME_LIB', 'DIPLOME_LIB_INT', 'DIPLOME_RGP', 'DISCIPLINE', 'DISCIPLINES_SELECTION',
                         'DISCIPLINE_LIB', 'DISCIPLINE_INT', 'DISCIPLINE_LIB_INT', 'DN_DE', 'DN_DE_LIB',
                         'ETABLISSEMENT_ACADEMIE', 'ETABLISSEMENT_ACTUEL_LIB', 'ETABLISSEMENT_CODE_COMMUNE',
                         'ETABLISSEMENT_COMMUNE', 'ETABLISSEMENT_COMPOS_ID_PAYSAGE', 'ETABLISSEMENT_COMPOS_LIB',
                         'ETABLISSEMENT_DEPARTEMENT', 'ETABLISSEMENT_ID_ACADEMIE', 'ETABLISSEMENT_ID_DEPARTEMENT',
                         'ETABLISSEMENT_ID_PAYSAGE', 'ETABLISSEMENT_ID_PAYSAGE_ACTUEL', 'ETABLISSEMENT_ID_REGION',
                         'ETABLISSEMENT_ID_ROR', 'ETABLISSEMENT_ID_UAI', 'ETABLISSEMENT_ID_UAI_SOURCE',
                         'ETABLISSEMENT_ID_UUCR', 'ETABLISSEMENT_ID_WIKIDATA', 'ETABLISSEMENT_LIB',
                         'ETABLISSEMENT_LOCALISATION', 'ETABLISSEMENT_REGION', 'ETABLISSEMENT_TYPE',
                         'ETABLISSEMENT_TYPOLOGIE', 'ETABLISSEMENT_UUCR', 'FORM_ENS_ID_PAYSAGE', 'FORM_ENS_LIB',
                         'GD_DISCIPLINE_INT', 'GD_DISCIPLINE', 'GD_DISCIPLINE_LIB', "GD_DISCIPLINE_LIB_INT",
                         'IMPLANTATION_ACADEMIE', 'IMPLANTATION_CODE_COMMUNE', 'IMPLANTATION_COMMUNE',
                         'IMPLANTATION_DEPARTEMENT', 'IMPLANTATION_ID_ACADEMIE', 'IMPLANTATION_ID_DEPARTEMENT',
                         'IMPLANTATION_ID_REGION', 'IMPLANTATION_ID_UUCR', 'IMPLANTATION_LOCALISATION',
                         'IMPLANTATION_REGION', 'IMPLANTATION_UUCR', 'OBTENU_DIPINT', 'OBTENU_DIPLOME',
                         'RENTREE', 'SECT_DISCIPLINAIRE', 'SECT_DISCIPLINAIRE_LIB', 'SECT_DISCIPLINAIRE_INT',
                         'SECT_DISCIPLINAIRE_LIB_INT', 'SESSION', 'SEXE', 'SEXE_LIB', 'SPEC_IUT', 'SPEC_IUT_LIB',
                         'SPEC_IUT_RGP_LIB'], as_index=False, dropna=False).agg(
        {'EFFECTIF_R': 'sum', 'EFFECTIF_INT': 'sum', 'EFFECTIF_TOTAL': 'sum', 'ATTRAC_INTERN_UE_27': 'sum',
         'ATTRAC_INTERN_OCDE_MEMBRES': 'sum', 'ATTRAC_INTERN_BOLOGNE': 'sum', 'ATTRAC_INTERN_BRICS': 'sum'})

    col_names = {"ANNEE_UNIVERSITAIRE": "Année universitaire",
                 "ATTRAC_INTERN_BOLOGNE": "Nombre de diplômes délivrés BOLOGNE",
                 "ATTRAC_INTERN_BRICS": "Nombre de diplômes délivrés BRICS",
                 "ATTRAC_INTERN_LIB": "Attractivité internationale",
                 "ATTRAC_INTERN_OCDE_MEMBRES": "Nombre de diplômes délivrés OCDE_MEMBRES",
                 "ATTRAC_INTERN_UE_27": "Nombre de diplômes délivrés UE27", "BAC": "bac",
                 "BAC_AGE_LIB": "Age au baccalauréat", "BAC_LIB": "Série du baccalauréat obtenu",
                 "BAC_RGP": "Type du baccalauréat obtenu",
                 "CURSUS_LMD": "CURSUS_LMD_R", "CURSUS_LMD_LIB": "Cycle universitaire (cursus LMD)",
                 "CURSUS_LMD_INT": "Cycle universitaire intermédiaire", "CURSUS_LMD_LIB_INT": "cursus_lmd_int",
                 "DIPLOME": "DIPLOME_r", "DIPLOME_INT": "DIPlome_int", "DIPLOME_LIB": "Diplôme délivré",
                 "DIPLOME_LIB_INT": "Diplôme intermédiaire délivré", "DIPLOME_RGP": "Regroupement de diplômes",
                 "DISCIPLINE": "DISCIPLINE_r", "DISCIPLINES_SELECTION": "Sélection de la discipline",
                 "DISCIPLINE_INT": "DISCIPLine_int", "DISCIPLINE_LIB": "Discipline du diplôme",
                 "DISCIPLINE_LIB_INT": "Discipline du diplôme intermédiaire", "DN_DE": "dn_de_r",
                 "DN_DE_LIB": "Type de diplôme",
                 "EFFECTIF_INT": "Nombre de diplômes intermédiaires délivrés",
                 "EFFECTIF_R": "Nombre de diplômes délivrés", "EFFECTIF_TOTAL": "Nombre total de diplômes délivrés",
                 "ETABLISSEMENT_ACADEMIE": "Académie de l'établissement",
                 "ETABLISSEMENT_ACTUEL_LIB": "etablissement_actuel_lib",
                 "ETABLISSEMENT_CODE_COMMUNE": "etablissement_code_commune",
                 "ETABLISSEMENT_COMMUNE": "Commune du siège de l'établissement",
                 "ETABLISSEMENT_COMPOS_ID_PAYSAGE": "etablissement_compos_id_paysage",
                 "ETABLISSEMENT_COMPOS_LIB": "Décomposition des universités",
                 "ETABLISSEMENT_DEPARTEMENT": "Département de l'établissement",
                 "ETABLISSEMENT_ID_ACADEMIE": "etablissement_id_academie",
                 "ETABLISSEMENT_ID_DEPARTEMENT": "etablissement_id_departement",
                 "ETABLISSEMENT_ID_PAYSAGE": "etablissement_id_paysage",
                 "ETABLISSEMENT_ID_PAYSAGE_ACTUEL": "etablissement_id_paysage_actuel",
                 "ETABLISSEMENT_ID_REGION": "etablissement_id_region", "ETABLISSEMENT_ID_ROR": "etablissement_id_ror",
                 "ETABLISSEMENT_ID_UAI": "etablissement_id_uai",
                 "ETABLISSEMENT_ID_UAI_SOURCE": "etablissement_id_uai_source",
                 "ETABLISSEMENT_ID_UUCR": "etablissement_id_UUCR",
                 "ETABLISSEMENT_ID_WIKIDATA": "etablissement_id_wikidata", "ETABLISSEMENT_LIB": "Etablissement",
                 "ETABLISSEMENT_LOCALISATION": "Localisation du siège de l'établissement",
                 "ETABLISSEMENT_REGION": "Région de l'établissement", "ETABLISSEMENT_TYPE": "Type d'établissement",
                 "ETABLISSEMENT_TYPOLOGIE": "Typologie d'établissement",
                 "ETABLISSEMENT_UUCR": "Unité urbaine ou commune rurale de l'établissement",
                 "FORM_ENS_ID_PAYSAGE": "form_ens_id_paysage", "FORM_ENS_LIB": "ESPE - INSPE",
                 "GD_DISCIPLINE": "GD_DISCIPLINE_r",
                 "GD_DISCIPLINE_LIB_INT": "Grande discipline du diplôme intermédiaire",
                 "GD_DISCIPLINE_INT": "GD_DISCIPLine_int", "GD_DISCIPLINE_LIB": "Grande discipline du diplôme",
                 "IMPLANTATION_ACADEMIE": "Academie de l'unité d'inscription",
                 "IMPLANTATION_CODE_COMMUNE": "implantation_code_commune",
                 "IMPLANTATION_COMMUNE": "Commune de l'unité d'inscription",
                 "IMPLANTATION_DEPARTEMENT": "Département de l'unité d'inscription",
                 "IMPLANTATION_ID_ACADEMIE": "implantation_id_academie",
                 "IMPLANTATION_ID_DEPARTEMENT": "implantation_id_departement",
                 "IMPLANTATION_ID_REGION": "implantation_id_region",
                 "IMPLANTATION_ID_UUCR": "implantation_id_uucr",
                 "IMPLANTATION_LOCALISATION": "Localisation de l'unité d'inscription",
                 "IMPLANTATION_REGION": "Région de l'unité d'inscription",
                 "IMPLANTATION_UUCR": "Unité urbaine ou commune rurale de l'unité d'inscription",
                 "OBTENU_DIPINT": "obtenu_dipint",
                 "OBTENU_DIPLOME": "obtenu_diplome", "RENTREE": "rentree",
                 "SECT_DISCIPLINAIRE": "SECT_DISCIPLINAIRE_r", "SECT_DISCIPLINAIRE_INT": "SECT_DISCIPL_int",
                 "SECT_DISCIPLINAIRE_LIB": "Secteur disciplinaire du diplôme",
                 "SECT_DISCIPLINAIRE_LIB_INT": "Secteur disicplinaire du diplôme intermédiaire", "SESSION": "Session",
                 "SEXE": "sexe", "SEXE_LIB": "Sexe de l'étudiant", "SPEC_IUT": "SPEC_DUTr",
                 "SPEC_IUT_LIB": "Spécialité du DUT délivré",
                 "SPEC_IUT_RGP_LIB": "Regroupement de spécialité DUTs du DUT"}

    OD = OD.rename(columns=col_names)

    return OD


def generate_od():
    logger.debug(f'start generation')
    liste2 = []
    for year in range(2015, 2022):
        liste = []
        logger.debug(f'start correction for {year}')
        sources = get_sources(year)
        for source in sources:
            start_main = time.time()
            logger.debug(f'start correction for {year} {source}')
            df = read_diplome(source=source, year=year)
            df = df.loc[df["EFFECTIF_TOT"] > 0]
            op150 = C_ETABLISSEMENT[["ID_PAYSAGE", "OPERATEUR_LOLF_150"]]
            df = pd.merge(df, op150, on="ID_PAYSAGE", how="left")
            df = df.loc[df["OPERATEUR_LOLF_150"] == "O"]
            df = df.drop(columns="OPERATEUR_LOLF_150")
            liste.append(df)
        liste_df = pd.concat(liste)
        df_sise = sise(liste_df)
        df_od = opendata19(df_sise)
        liste2.append(df_od)
        df_od.to_csv(f"{DATA_PATH}/od/od_diplomes_{year}.csv", index=False, encoding='utf-8', sep=";")
        swift.upload_object_path("sas", f"{DATA_PATH}/od/od_diplomes_{year}.csv")
        print(f"duration cleaning {year} -> {time.time() - start_main}")

    dod = pd.concat(liste2)
    dod.to_csv(f"{DATA_PATH}/od/od_diplomes.csv", index=False, encoding='utf-8', sep=";")
    swift.upload_object_path("sas", f"{DATA_PATH}/od/od_diplomes.csv")

    logger.debug('done')