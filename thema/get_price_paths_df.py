import os
import pandas as pd
import re

"""
returnerer df med kolonner aar, path_prisfil

path_model_dirs - sti til mappe med thema-mapper for hvert vaeraar
model_year- str med navn paa scenario (eks 'B2020')

"""

def get_price_paths_df_tt(path_model_dirs, model_year):
    # for Thema kjøring med Themathon

    path_model_dirs = os.path.join(path_model_dirs, "Scenario_Output")
    
    rows = []


    for fn in os.listdir(path_model_dirs):

        file = re.findall(f"{model_year}_HY...._Prices.txt", fn)

        if len(file) > 0:
            file_path = os.path.join(path_model_dirs, file[0])
            vaeraar = int(re.findall('HY\d{4}', fn)[0][-4:])

            rows.append((vaeraar, file_path))


    df = pd.DataFrame(rows, columns=["aar", "path_prisfil"])

    return df


def get_price_paths_df(path_model_dirs, model_year):
    # for Thema kjøring uten Themathon

    rows = []

    for fn in os.listdir(path_model_dirs):
        path = os.path.join(path_model_dirs, fn)

        if not os.path.isdir(path):
            continue

        if not fn.isdigit():
            continue

        aar = int(fn)
        model_dir = os.path.abspath(path)

        file_path = os.path.join(model_dir, "Output", "Scenario_Output", "%s_Prices.txt" % model_year)

        rows.append((aar, file_path))

    df = pd.DataFrame(rows, columns=["aar", "path_prisfil"])

    return df