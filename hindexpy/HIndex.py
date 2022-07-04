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
        if subject == np.nan: 
            subject = 'nan'
        
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
