import os

from nve_modell.sintef.io.prisrekke.skriv_prisrekke_fil import skriv_prisrekke_fil

from nve_modell.sintef.utils.get_uketime_tsnitt_map import get_uketime_tsnitt_map
from nve_modell.sintef.utils.get_aar_kobling_df     import get_aar_kobling_df

from .get_price_df                       import get_price_df
from .get_price_paths_df                 import *

def skriv_prisrekkefiler(themathon, output_dir, eurnok, thema_models_dir, thema_model_year_name, kobling_thema_emps,
                         antall_tsnitt, aar_liste_input, antall_uker_dataperiode, aar_liste_output=None, parallell_serie=0):
    """
    lager liste med input som kan sendes til skriv_prisrekke for aa lage prisrekke-filer

    output_dir - sti til mappe hvor prisrekkefilene skal skrives

    eurnok - nok per eur
    thema_models_dir - sti til mappe hvor thema-kjoringer for hvert vaeraar ligger
    thema_model_year_name - navn paa modellaar i thema (eks. 'B2020')
    antall_tsnitt - antall tidsavsnitt paa prisrekke-filen som skal lages
    aar_liste_input - liste med vaeraar fra thema-modellen som skal brukes til aa lage prisrekker 
                      ( eks. list(range(1981, 2011)) for perioden 1981-2010)
    antall_uker_dataperiode - antall uker i dataperioden i prisrekke-filen (eks. 156)
    aar_liste_output - liste med aar som skal vaere med i prisrekke-filen (eks. list(range(1958, 2017)) for 1958-2016) sekvensen
                       med aar fra aar_liste_input vil bli repetert for aarene som ikke har blitt simulert med thema-modellen
    parallell_serie = 0 hvis prisrekken skal ha parallell-modus, 1 hvis serie (default 0)

    kobling_thema_emps - dict som beskriver koblingen mellom omrnavn i thema-modellen og omrnavn i emps-modellen (se eks under) 

                            eksempel:

                            kobling_thema_emps = dict()
                            kobling_thema_emps["GER"]       = "TYSKLAND"
                            kobling_thema_emps["NET"]       = "NEDERLAND"
                            kobling_thema_emps["POL"]       = "POLEN"
                            kobling_thema_emps["LIT"]       = "LITAUEN"
                            kobling_thema_emps["EST"]       = "ESTLAND"
                            kobling_thema_emps["GBR"]       = "UK"
                            kobling_thema_emps["EX_RUS"]    = "RUSSLAND_EKS"
                            kobling_thema_emps["EX_RUS_CM"] = "RUSSLAND_IMP"

    """

    uketime_tsnitt_map = get_uketime_tsnitt_map(antall_tsnitt)

    startaar_inn = min(aar_liste_input)
    sluttaar_inn = max(aar_liste_input)

    if aar_liste_output:
        startaar_ut = min(aar_liste_output)
        sluttaar_ut = max(aar_liste_output)
    else:
        startaar_ut = startaar_inn
        sluttaar_ut = sluttaar_inn

    aar_kobling_df = get_aar_kobling_df(startaar_inn, sluttaar_inn, startaar_ut, sluttaar_ut)

    if themathon == False:
        price_paths_df = get_price_paths_df(thema_models_dir, thema_model_year_name)

    if themathon == True:
        price_paths_df = get_price_paths_df_tt(thema_models_dir, thema_model_year_name)


    price_paths_df = price_paths_df[price_paths_df["aar"].isin(aar_liste_input)]

    price_df = get_price_df(price_paths_df)
    price_df = price_df.rename(columns={"aar" : "aar_kobling"})

    price_df = price_df.merge(aar_kobling_df, on="aar_kobling")
    del price_df["aar_kobling"]

    price_df = price_df.sort_values(by=["aar", "uke", "uketime"])
    price_df = price_df.reset_index(drop=True)
    price_df = price_df.set_index(["aar", "uke", "uketime", "aartime"])
    price_df = price_df.reset_index(drop=False)

    inputs = []
    
    for thema_area, emps_area in kobling_thema_emps.items():
        df = price_df.copy()
        df = df[["aar", "uke", "uketime", thema_area]]
        df = df.rename(columns={thema_area : "pris"})

        df["pris"] = df["pris"]*eurnok/10.0

        path = os.path.join(output_dir, "%s.PRI" % emps_area)
        path = os.path.abspath(path)

        d = dict()
        d["path"] = path
        d["price_df"] = df
        d["uketime_tsnitt_map"] = uketime_tsnitt_map.copy()
        d["antall_uker_dataperiode"] = antall_uker_dataperiode
        d["parallell_serie"] = parallell_serie

        inputs.append(d)

    os.makedirs(output_dir, exist_ok=True)

    for d in inputs:
        path = d["path"]
        price_df = d["price_df"]
        antall_uker_dataperiode = d["antall_uker_dataperiode"]
        parallell_serie = d["parallell_serie"] 

        print("skriver %s" % path)

        skriv_prisrekke_fil(path, uketime_tsnitt_map, price_df, antall_uker_dataperiode, parallell_serie)

    print("")


