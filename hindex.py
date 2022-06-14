import numpy as np
from pandas import Series

class SubjectCitation:
    def __init__(self):
        self.elements = {}
        
    def __len__(self):
        return len(self.elements)
    
    def _is_subject(self, subject):
        # if there is already a subject in the heap, return True
        if subject in self.elements:
            return True
        else:
            return False
    
    def put(self, subject, citation_count):
        # if subject is nan, name it as 'nan'
        if subject == np.nan: subject = 'nan'
        
        if self._is_subject(subject):
            # if there is already a subject in the heap, update the count
            self.elements[subject].append(citation_count)
        else:
            # if there is no subject, create a new one
            self.elements[subject] = [citation_count]
        
    def clear(self):
        self.elements = {}
        
    def to_dict(self):
        return self.elements
    
    def __str__(self):
        return str(self.elements)
    
    def __repr__(self):
        return str(self.elements)
    
    def __iter__(self):
        return iter(self.elements)
    
    def __getitem__(self, key):
        return self.elements[key]
    
    
def h_index(citations):
    '''
    Count the H-index of a given author.
    
    INPUT: citations (list of integers)
    OUTPUT: h-index (int)
    
    Ref: https://en.wikipedia.org/wiki/H-index
    '''
    
    citations = sorted(citations, reverse=True)
    h_index_value = 0
    for i, c in enumerate(citations):
        if c >= i + 1:
            h_index_value = i + 1
    return h_index_value

def h_index_from_dict(dct):
    '''
    Compute the h-index from a dictionary with values as lists.
    
    INPUT: dict with citation (dict)
    OUTPUT: dict with h-index (dict)
    '''
    
    for k, v in dct.items():
        dct[k] = h_index(v)
    
    return dct


def h_index_by_subject(data, id_list, id_colname, cit_colname, select_colname):
    '''
    Compute the h-index with the consideration of faculty area.

    INPUT: data (dataframe), 
           id_list (list),
           cit_col - the name of the columein data which records the citation counts (string), 
           select_col - the name of the columein data which records the categorical information (string)
    OUTPUT: h-index (series of dictionary {subject: [citation_count1, citation_count2, ...]})
    '''    
     
    author_data_list = [] # {author_handle: {'subject_id': [citation_count1, citation_count2, ...]}}

    ## compute the h-index with the consideration of asjcs
    for id in id_list:
        # get the list of asjcs for the author
        prog = data[data[id_colname] == id][select_colname].tolist()
        author_info = SubjectCitation()
        
        index = 0
        # for each asjcs, put the citation count into the SubjectCitation object
        for sett in prog:
            for subject in sett:
                author_info.put(subject, data.iloc[index][cit_colname])
            index += 1
        
        author_data_list.append(author_info.to_dict())
        
    return Series(author_data_list)

def h_index_from_series_of_dict(ser, dropna=False):
    '''
    to compute the h-index for each author
    
    INPUT: ser (series)
    OUTPUT: h-index (series)
    '''
    ser = ser.copy()
    if dropna:
        for index in range(len(ser)):
            for subject, cit in ser[index].items():
                # compute the h-index with the consideration of asjcs
                h_index_value = h_index(cit)
                ser[index][subject] = h_index_value
        
        # drop the 'NaN' keys of dictionaries in the rows
        ser = ser.apply(lambda x: {k: v for k, v in x.items() if k != 'NaN'})
    
    else:
        for index in range(len(ser)):
            for subject, cit in ser[index].items():
                # compute the h-index with the consideration of asjcs
                h_index_value = h_index(cit)
                ser[index][subject] = h_index_value
            
        
    return ser


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


def dict_to_list(id_list, target):
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

def df_to_dict(df, key_col, val_col):
    '''
    Convert a dataframe to a dictionary.
    
    INPUT: df (dataframe), key_col (string), val_col (string)
    OUTPUT: dictionary
    '''
    
    return dict(zip(df[key_col], df[val_col]))
