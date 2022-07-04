import pandas as pd
import numpy as np

def filter_sort_and_drop_duplicates(dataframe: pd.DataFrame, 
                         sort_colname: str, 
                         drop_dup = False):
    '''
    To clean the dataframe by sorting and/or drop duplicated rows
    
    INPUT:
        dataframe: the targeted dataframe
        sort_colname: the column name used as a pivot of sorting; 
                        the value in such a column is suggested to be interger or float
        drop_dup: if True, will drop; default to be False. 
    
    OUTPUT: 
        dataframe: cleaned dataframe
    
    MODULE: 
        pandas
    '''
    dataframe = dataframe.sort_values(by=sort_colname)
    
    if drop_dup:
        dataframe = dataframe.drop_duplicates()
    
    dataframe = dataframe.reset_index(drop=True)
    
    return dataframe

def filter_useless_column(dataframe: pd.DataFrame, colname_list: list, dropna = 'False'):
    dataframe = dataframe[colname_list]
    
    match dropna:
        case 'False':
            pass
        case 'all':
            dataframe = dataframe.dropna(how='all')
        case 'any':
            dataframe = dataframe.dropna()
            
    return dataframe

# A function to filter datetime values
def filter_year_range(dataframe, colname, begin_year, end_year):
    dataframe = dataframe[dataframe[colname].dt.year >= begin_year] # filter if larger or equal than the begin year
    dataframe = dataframe[dataframe[colname].dt.year <= end_year] # filter if less or equal than the end year
    dataframe.reset_index(drop=True, inplace=True) # reset the index
    
    year_list = list(range(begin_year, end_year+1)) # create a year list for process afterwards, if needed
    
    return dataframe, year_list


def categorise_citation_by_map(dataframe: pd.DataFrame, 
                               map_col: str, 
                               citation_col: str, 
                               option: str = None, 
                               map_object: dict = None, author_col: str = None):
    '''
    This function categorise the citation by the subject categories
    
    Parameters
    ----------
    dataframe : pd.DataFrame
        The dataframe containing the citation data
    map_col : str
        The column name of the category
    citation_col : str
        The column name of the citation
    option : str, optional
        The option to identify which category to use. The options are:
            1. 'normal': categorise the citation by the map_col
            1. 'map' : use the map object to categorise the citation as well;
            2. 'author' : use the author to categorise the citation as well, and it will output a nested dictionary with the first key of author information, and then, of subject information; or
            3. 'both' : use the map object and the author to categorise the citation as well.
        Otherwise, this function will not categorise.
    map_object : dict, optional
        The map object containing the mapping between ASJC and QS subject categories. The default is None.
        **Notice**: you need to write 'map_object=*map_object*' to use this parameter.
    author_col : str, optional
        The column name of the author. The default is None.
        **Notice**: you need to write 'author_col=*author_col*' to use this parameter.
    
    Returns
    -------
    dict_citation : dict
        The dictionary containing the citation by category
    
    Modules
    -------
    pandas, numpy
    '''
    assert option is not None, 'Please specify the option to categorise the citation'
    
    match option:
        case None:
            cat_list = dataframe[map_col].to_list() # since the values are list and not hashable
            cat_list = {c for sublist in cat_list for c in sublist} # get the unique values
            dict_citation = {cat:[] for cat in cat_list} # make a dictionary to store the citation by category
            
            # fill the dict by the citation of each category from the dataframe
            for i in range(len(dataframe)):
                cat_list_dataframe = dataframe[map_col].iloc[i]
                for cat in cat_list_dataframe:
                    dict_citation[cat].append(dataframe[citation_col].iloc[i])
        
        case 'map':
            cat_list = dataframe[map_col].to_list() # since the values are list and not hashable
            cat_list = {map_object[c] for sublist in cat_list for c in sublist if c in map_object} # get the unique values
            dict_citation = {cat:[] for cat in cat_list} # make a dictionary to store the citation by category
            
            # fill the dict by the citation of each category from the dataframe
            for i in range(len(dataframe)):
                cat_list_dataframe = dataframe[map_col].iloc[i]
                for cat in cat_list_dataframe:
                    if cat in map_object:
                        dict_citation[map_object[cat]].append(dataframe[citation_col].iloc[i])
        
        case 'author':
            author_list = dataframe[author_col].unique() # list unique values in column 'handle'
            dict_citation = {author: {} for author in author_list} # make a dictionary to store the citation by category

            # fill the dict by the citation of each category from the dataframe
            for author in author_list:
                author_data = dataframe[dataframe[author_col] == author]
                author_cat_list = author_data[map_col].to_list() # since the values are list and not hashable
                author_cat_list = {cat for sublist in author_cat_list for cat in sublist} # remove duplicates
                author_cat_data = {cat:[] for cat in author_cat_list} # make a dictionary to store the citation by category
                for cat in author_cat_data:
                    for index in range(len(author_data)):
                        if cat in author_data.iloc[index][map_col]:
                            author_cat_data[cat].append(author_data.iloc[index][citation_col])
                
                dict_citation[author] = author_cat_data # store the citation by category in the dictionary

        case 'both':
            author_list = dataframe[author_col].unique() # list unique values in column 'handle'
            dict_citation = {author:{} for author in author_list} # make a dictionary to store the citation by category

            # fill the dict by the citation of each category from the dataframe
            for author in author_list:
                author_data = dataframe[dataframe[author_col] == author]
                author_cat_list = author_data[map_col].to_list() # since the values are list and not hashable
                author_cat_list = {cat for sublist in author_cat_list for cat in sublist if cat in map_object} # remove duplicates
                author_cat_data = {map_object[cat]:[] for cat in author_cat_list if cat in map_object} # make a dictionary to store the citation by category
                for cat in author_cat_list:
                    for index in range(len(author_data)):
                        if cat in author_data.iloc[index][map_col]:
                            author_cat_data[map_object[cat]].append(author_data.iloc[index][citation_col])
                    
                dict_citation[author] = author_cat_data # store the citation by category in the dictionary 
                    
    return dict_citation
