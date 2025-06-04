import json
import os

import pandas as pd
from application.server.main.logger import get_logger

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")


def get_all_correctifs_from_google():
    DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")
    url = f'https://docs.google.com/spreadsheet/ccc?key={os.getenv("KEY_GGSHT")}&output=xls'
    CORRECTIFS_dict = {}
    VARS = ['ETABLI', 'A_UAI', 'C_ETABLISSEMENTS', 'D_EPE', 'E_FORM_ENS', 'DEP_ACA_RESPA_CORRECTIF',
            'F_RENTREES', 'G_PAYS', 'H_PROXIMITE', 'I_DNDU', 'J_LMDDONT', 'DISCIPLINES_SISE', 'ETABLI_SOURCE',
            'L_ED', 'M_IUT', 'N_ING', 'O_DUTBUT', 'LES_COMMUNES', 'DEPTOREG', 'CORRLMD', 'DEPTOREGNEW',
            'ETABLI_DIFFUSION_ID', 'FORMATIONS_CORRECTIF', 'CURSUS_LMD_CORRECTIF', 'RESTE_DEPRESPA_CORRECTIF',
            'DEP_CORRECTIF', 'ACA_CORRECTIF', 'GROUPE_CORRECTIF', 'COMINS', 'COMUI', 'COMETAB', 'deleter',
            'K_FORM_ENS_ETAB', 'SEXE', 'BAC_RGRP', 'AVANCE_RETARD', 'PROXBAC', 'PROXREGBAC', 'ATTRAC_INTERN',
            'MOBILITE_INTERN', 'DNDU', 'CURSUS_LMD', 'LMDDONTBIS', 'NIVEAU', 'SECTDIS', 'SPECIUT']
    df_c = pd.read_excel(url, sheet_name=VARS, dtype=str, na_filter=False)
    for VAR in VARS:
        correctifs = df_c.get(VAR).to_dict(orient='records')
        for c in correctifs:
            for f in c:
                if c[f] != c[f]:  # nan
                    c[f] = ''
                if 'annee' in f.lower() or 'rentree' in f.lower():
                    c[f] = str(c[f])
                if isinstance(c[f], str):
                    c[f] = c[f].split('.0')[0].strip()
                elif isinstance(c[f], float) or isinstance(c[f], int):
                    c[f] = str(c[f]).split('.0')[0].strip()

        CORRECTIFS_dict[f'{VAR}'] = correctifs
    json.dump(CORRECTIFS_dict, open(f'{DATA_PATH}correctifs.json', 'w'))


def get_all_correctifs(chx: str) -> dict:
    DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")
    if chx in ["google", "json"]:
        if chx == "google":
            get_all_correctifs_from_google()
        elif chx == "json":
            if "correctifs.json" in os.listdir(DATA_PATH):
                pass
            else:
                raise ValueError(
                    "le fichier correctifs.json n\'existe pas encore ; il faut lancer le chargement google")
        with open(f'{DATA_PATH}correctifs.json', "r") as f:
            file = json.load(f)
        correctifs = file.copy()

        for key in correctifs.keys():
            df = pd.DataFrame(data=correctifs[key])
            df = df.drop_duplicates()
            df.columns = df.columns.str.upper()
            dico = df.to_dict(orient="records")
            correctifs[key] = dico
    else:
        raise ValueError("Choix autre que google ou json")

    return correctifs
