import pandas as pd

def get_price_df(price_paths_df):
    """
    Leser thema_prisfiler for hvert vaeraar og slaar
    alt sammen til en stor df
    
    df har indeks ["aar", "uke", "uketime", "aartime"] 
    
           og verdi-kolonnene er hvert omr i thema-modell
           og verdiene er pris i eur/mwh
    
    """
    
    dfs = []
    
    for __,r in price_paths_df.iterrows():
        aar = r["aar"]
        path = r["path_prisfil"]
        
        df = pd.read_csv(path, low_memory=False)
        
        df = df[df["MP"] != "AV"]
        df = df[df["LB"] != "AV"]
        
        df["MP"] = df["MP"].astype(int)
        df["LB"] = df["LB"].astype(int)
        
        df["aar"] = aar
        
        df = df.reset_index(drop=True)
        df["aartime"] = df.index + 1
       
        dfs.append(df)
        
    df = pd.concat(dfs)
    df = df.reset_index(drop=True)

    df = df.rename(columns={"MP" : "uke", "LB" : "uketime"})
    df = df.set_index(["aar", "uke", "uketime", "aartime"])
    df = df.reset_index()
    
    return df
