import numpy as np
import pandas as pd
import os
import time
import json
from unidecode import unidecode


DATA_PATH = "/run/media/julia/DATA/diplomes_donnees/"
os.chdir(DATA_PATH)

df_od = pd.read_csv(f"{DATA_PATH}/od/od_diplomes.csv", encoding='utf-8', sep=";", engine="python",
                    dtype={"rentree": str})

df_od.loc[df_od["etablissement_id_paysage"] == "pVJpw", "Type d\'établissement"] = "Université"

df_od["comp_etab"] = df_od.apply(lambda a: "même établissement" if a["etablissement_id_paysage"] == a[
    "etablissement_id_paysage_actuel"] else "établissement différent", axis=1)
df_od["decomp"] = df_od.apply(lambda a: "même établissement" if a["etablissement_id_paysage"] == a[
    "etablissement_compos_id_paysage"] else "établissement différent", axis=1)

tab1 = df_od.loc[(df_od["Décomposition des universités"].isna()) | (df_od["decomp"]=="même établissement") | (
        df_od["etablissement_id_paysage"] == "XIGGw"), ["Etablissement",
                                                        "Session",
                                                        "etablissement_id_paysage",
                                                        "Type d'établissement", "etablissement_id_paysage_actuel",
                                                        "etablissement_actuel_lib", "comp_etab"]]
for col in ["Etablissement", "Session", "etablissement_id_paysage", "Type d'établissement",
            "etablissement_id_paysage_actuel", "etablissement_actuel_lib", "comp_etab"]:
    tab1[col] = tab1[col].fillna("")
tab1_min = tab1.groupby(
    ["etablissement_id_paysage", "Etablissement", "etablissement_id_paysage_actuel", "etablissement_actuel_lib",
     "Type d'établissement", "comp_etab"]).min().reset_index().rename(
    columns={"Session": "debut"})
tab1_max = tab1.groupby(
    ["etablissement_id_paysage", "Etablissement", "etablissement_id_paysage_actuel", "etablissement_actuel_lib",
     "Type d'établissement", "comp_etab"]).max().reset_index().rename(
    columns={"Session": "fin"})
tab1_session = pd.merge(tab1_min, tab1_max,
                        on=["etablissement_id_paysage", "Etablissement", "etablissement_id_paysage_actuel",
                            "etablissement_actuel_lib",
                            "Type d'établissement", "comp_etab"], how="outer")
tab1_session.loc[tab1_session["debut"] == tab1_session["fin"], "unique"] = "unique"
tab1_session.loc[tab1_session["unique"].isna(), "unique"] = "non"
tab1 = pd.merge(tab1, tab1_session, on=["etablissement_id_paysage", "Etablissement", "etablissement_id_paysage_actuel",
                                        "etablissement_actuel_lib",
                                        "Type d'établissement", "comp_etab"],
                how="outer").drop(
    columns="Session").drop_duplicates().reset_index(drop=True)
tab1["Sessions"] = tab1.apply(
    lambda a: str(a["debut"]) + " à " + str(a["fin"]) if a["unique"] == "non" else str(a["debut"]), axis=1)
tab1.loc[tab1["comp_etab"] == "même établissement", "Etablissement actuel (si établissement observé inactif)"] = ""
tab1.loc[tab1[
    "Etablissement actuel (si établissement observé inactif)"].isna(), "Etablissement actuel (si établissement observé inactif)"] = \
    tab1.loc[tab1["Etablissement actuel (si établissement observé inactif)"].isna(), "etablissement_actuel_lib"]

tab1.loc[tab1["comp_etab"] == "même établissement", "etablissement_id_paysage_actuel"] = ""

tab1["uni1"] = tab1["Etablissement"].apply(lambda a: unidecode(a).lower())
tab1["uni2"] = tab1["etablissement_actuel_lib"].apply(lambda a: unidecode(a).lower())

tab1 = tab1[["etablissement_id_paysage", "Etablissement", "Sessions", "etablissement_id_paysage_actuel",
             "Etablissement actuel (si établissement observé inactif)", "uni1", "uni2"]].drop_duplicates().sort_values(
    ["uni1", "uni2", "Sessions"]).drop(columns=["uni1", "uni2"]).reset_index(drop=True)

tab1.to_csv("od/stats_meme_etab.csv", sep=";", index=False)

tab2 = df_od.loc[df_od["Décomposition des universités"].notna(), [
    "etablissement_id_paysage", "Etablissement", "Session",
    "Type d'établissement", "etablissement_id_paysage_actuel",
    "etablissement_actuel_lib", "Décomposition des universités", "etablissement_compos_id_paysage"]]
tab2 = tab2.loc[df_od["etablissement_id_paysage"] != "XIGGw"]
tab2.loc[
    tab2["etablissement_compos_id_paysage"].isna(), "Décomposition d'EPE et de grands établissements issus d'un EPE"] = \
    tab2.loc[tab2["etablissement_compos_id_paysage"].isna(), "Etablissement"]
tab2.loc[tab2["etablissement_compos_id_paysage"].isna(), "paysage"] = tab2.loc[
    tab2["etablissement_compos_id_paysage"].isna(), "etablissement_id_paysage"]

tab2.loc[
    tab2[
        "Décomposition d'EPE et de grands établissements issus d'un EPE"].isna(), "Décomposition d'EPE et de grands établissements issus d'un EPE"] = \
    tab2.loc[
        tab2["Décomposition d'EPE et de grands établissements issus d'un EPE"].isna(), "Décomposition des universités"]

tab2_min = tab2.groupby(
    ["etablissement_id_paysage_actuel", "etablissement_actuel_lib", "etablissement_compos_id_paysage",
     "Décomposition d'EPE et de grands établissements issus d'un EPE",
     "Type d'établissement"]).min().reset_index().rename(
    columns={"Session": "debut"})
tab2_max = tab2[
    ["etablissement_compos_id_paysage", "Décomposition d'EPE et de grands établissements issus d'un EPE", "Session", "Type d'établissement",
     "etablissement_id_paysage_actuel",
     "etablissement_actuel_lib"]].groupby(
    ["etablissement_id_paysage_actuel", "etablissement_actuel_lib", "etablissement_compos_id_paysage",
     "Décomposition d'EPE et de grands établissements issus d'un EPE",
     "Type d'établissement"]).max().reset_index().rename(
    columns={"Session": "fin"})
tab2_session = pd.merge(tab2_min, tab2_max,
                        on=["etablissement_id_paysage_actuel", "etablissement_actuel_lib", "etablissement_compos_id_paysage",
                            "Décomposition d'EPE et de grands établissements issus d'un EPE",
                            "Type d'établissement"], how="outer")
tab2_session.loc[tab2_session["debut"] == tab2_session["fin"], "unique"] = "unique"
tab2_session.loc[tab2_session["unique"].isna(), "unique"] = "non"
tab2 = pd.merge(tab2, tab2_session, on=["etablissement_id_paysage_actuel", "etablissement_actuel_lib", "etablissement_compos_id_paysage",
                                        "Décomposition d'EPE et de grands établissements issus d'un EPE",
                                        "Type d'établissement"],
                how="outer").drop(
    columns="Session").drop_duplicates().reset_index(drop=True)
tab2["Sessions"] = tab2.apply(
    lambda a: str(a["debut"]) + " à " + str(a["fin"]) if a["unique"] == "non" else str(a["debut"]), axis=1)

tab2["uni1"] = tab2["etablissement_actuel_lib"].apply(lambda a: unidecode(a).lower())
tab2["uni2"] = tab2["Décomposition d'EPE et de grands établissements issus d'un EPE"].apply(lambda a: unidecode(a).lower())

tab2 = tab2[["etablissement_id_paysage_actuel", "etablissement_actuel_lib", "Sessions", "etablissement_compos_id_paysage",
             "Décomposition d'EPE et de grands établissements issus d'un EPE", "uni1", "uni2"]].drop_duplicates().sort_values(
    ["uni1", "uni2", "Sessions"]).drop(columns=["uni1", "uni2"]).reset_index(drop=True)

tab2.to_csv("od/stats_diff_etab_epe.csv", sep=";", index=False)

types_etab = list(df_od["Type d'établissement"].unique())
types_etab.sort()

typo_etab = list(df_od.loc[df_od["Typologie d'établissement"].notna(), "Typologie d'établissement"].unique())
typo_etab.sort()

age_bac = list(df_od.loc[df_od["Age au baccalauréat"].notna(), "Age au baccalauréat"].unique())
