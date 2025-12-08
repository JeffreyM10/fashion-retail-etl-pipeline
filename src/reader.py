import pandas as pd

def read_csv(path: str):
    """
    Basic CSV reader: 
    - loads CSV into a pandas DataFrame -=
    - strips the col names from whitespaces
    - we want to lowercase all of the values
    """
    df = pd.read_csv(path) #dataframe 
    
    # After loading the CSV we normalize by lowercase, stip spacing, remove special chars
    df.columns = [c.strip().lower() for c in df.columns]  #for each c normalize and add it to col arr
    
    return df
