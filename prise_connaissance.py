import os
import numpy as np
import pandas as pd
import dtype_diplomes as typesd

DATA_PATH = "/run/media/julia/DATA/diplomes/"

os.chdir(DATA_PATH)

def annee(df: pd.DataFrame):
    col_an = ["ANBAC", "ANNAIS", "ANINSC", "ACARESPA", "DEPRESPA"]
    for col in col_an:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df.loc[df[col].notna(), col] = df.loc[df[col].notna(), col].str.replace(".0", "", regex=False)

    return df

culture = pd.read_parquet("dipculture21_ssa.parquet")
culture = culture.astype(typesd.types["culture21"])
culture = annee(culture)

enq = pd.read_parquet("dipenq26bis21_ssa.parquet")
enq = enq.astype(typesd.types["culture21"])
enq = annee(enq)

ens = pd.read_parquet("dipens21_ssa.parquet")
ens = ens.astype(typesd.types["ens21"])
ens = annee(ens)

inge = pd.read_parquet("dipinge21_ssa.parquet")
inge = inge.astype(typesd.types["culture21"])
inge = annee(inge)

mana = pd.read_parquet("dipmana21_ssa.parquet")
mana = mana.astype(typesd.types["culture21"])
mana = annee(mana)

priv = pd.read_parquet("dippriv21_ssa.parquet")
priv = priv.astype(typesd.types["priv21"])
priv = annee(priv)

result = pd.read_parquet("result21_ssa.parquet")
result = result.astype(typesd.types["result21"])
result = annee(result)