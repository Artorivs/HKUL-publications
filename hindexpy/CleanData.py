import pandas as pd

def rearrange_dict(handle_list, target):
    '''
    Rearrange the dictionary by the handle of the author.
    
    INPUT: handle_list (lst/array of strings), 
           target (dictionary)
    OUTPUT: list
    '''
    
    new_list = []
    
    for handle in handle_list:
        if handle in target:
            new_list.append(target[handle])
        else:
            new_list.append('NaN')
    
    return new_list


def convert_dict_to_list(id_list, target):
    '''
    Rearrange the dictionary by the handle of the author.
    
    INPUT: handle list (lst/array of strings), 
           target (dictionary)
    OUTPUT: list
    '''
    
    new_list = []
    
    for handle in id_list:
        if handle in target:
            new_list.append(target[handle])
        else:
            new_list.append('NaN')
    
    return new_list

def convert_df_to_dict(df, key_col, val_col):
    '''
    Convert a dataframe to a dictionary.
    
    INPUT: df (dataframe), key_col (string), val_col (string)
    OUTPUT: dictionary
    '''
    
    return dict(zip(df[key_col], df[val_col]))

def convert_setOfString_to_list(dataframe: pd.DataFrame, colname: str):
    '''
    to convert a column value from set-like string to list
        Further Information: 
            this function is designed for a dataframe with a column of string values, 
            where those values tend to be read as set values but somehow failed
    
    Parameters
    ----------
    dataframe : pd.DataFrame
        The dataframe containing the citation data
    colname : str
        The column name of the column to be converted
        
    Returns
    -------
    dataframe : pd.DataFrame
        The dataframe with the converted column values
        
    Modules
    -------
    pandas, numpy
    '''
    
    lambda_str_to_lst = lambda x: str(x).replace('{','').replace('}','').replace('"','').replace("'",'').split(',')
    dataframe[colname] = dataframe[colname].apply(lambda_str_to_lst)
    
    return dataframe


def convert_pipeSeperatedString_to_list(dataframe: pd.DataFrame, col_name: str) -> pd.DataFrame:
    dataframe[col_name] = dataframe[col_name].apply(lambda x: x.replace(' ', '').split('|'))
    
    return dataframe

def convert_column_datetime(dataframe: pd.DataFrame, date_colname:str, date_format=None):
    
    if date_format is None:
        dataframe[date_colname] = pd.to_datetime(dataframe[date_colname])
    else:
        dataframe[date_colname] = pd.to_datetime(dataframe[date_colname], format=date_format)
    
    return dataframe
