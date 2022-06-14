
from hindex import SubjectCitation, h_index_from_dict, dict_to_list, df_to_dict, h_index_by_subject
import pandas as pd
import json
from copy import deepcopy


dtype = {'Title':str,
         'Authors':str,
         'Number of Authors':str,
         'Scopus Author Ids':str,
         'Year':str,
         'Scopus Source title':str,
         'Volume':str,
         'Issue':str,
         'Pages':str,
         'Article number':str,
         'ISSN':str,
         'Source ID':str,
         'Source type':str}
# ,SNIP (publication year),SNIP percentile (publication year) *,CiteScore (publication year),CiteScore percentile (publication year) *,SJR (publication year),SJR percentile (publication year) *,Field-Weighted View Impact,Views,Citations,Field-Weighted Citation Impact,Field-Citation Average,"Outputs in Top Citation Percentiles, per percentile","Field-Weighted Outputs in Top Citation Percentiles, per percentile",Patent citations,Reference,Abstract,DOI,Publication type,Open Access,EID,PubMed ID,Institutions,Number of Institutions,Scopus Affiliation IDs,Scopus Affiliation names,Scopus Author ID First Author,Scopus Author ID Last Author,Scopus Author ID Corresponding Author,Scopus Author ID Single Author,Country/Region,All Science Journal Classification (ASJC) code,All Science Journal Classification (ASJC) field name,Sustainable Development Goals (2021),Topic Cluster name,Topic Cluster number,Topic name,Topic number,Topic Cluster Prominence Percentile,Topic Prominence Percentile}

df = pd.read_csv('./dataset/university/HKU2016-2021.csv',dtype=dtype)
df.Year = pd.to_datetime(df.Year, format='%Y')


### clean asjc-qs data
df_qs_subject = pd.read_csv('./dataset/ASJC to QS_Sept2020/ASJC - QS (Subj Map).csv')
df_qs_faculty = pd.read_csv('./dataset/ASJC to QS_Sept2020/ASJC - QS faculty areas.csv')


df_qs_subject = df_qs_subject.sort_values(by='ASJC code').reset_index(drop=True)
df_qs_faculty = df_qs_faculty.sort_values(by='ASJC code').reset_index(drop=True)


df_qs_faculty = df_qs_faculty.drop_duplicates(subset='ASJC code', keep='first')


dict_qs_subject = df_to_dict(df_qs_subject, 'ASJC code', 'QS subject name')
dict_qs_faculty = df_to_dict(df_qs_faculty, 'ASJC code', 'QS faculty area name')


### clean university data


# make a copy on several cols of df
col_lst = ['Year', 'Citations', 'All Science Journal Classification (ASJC) code']
temp = df[col_lst]
temp.columns = ['year', 'citation', 'asjc']

temp.asjc = temp.asjc.apply(lambda x: x.replace(' ', '').split('|'))

### 2016-2020
df_2016 = temp[temp.year >= '2016']
df_2016 = df_2016[df_2016.year <= '2020']

list_2016 = df_2016.asjc.to_list()
set_2016 = set([item for sublist in list_2016 for item in sublist])

dict_2016 = {asjc:[] for asjc in set_2016}

# fill the dict by the citation of each ASJC from the dataframe
for i in range(len(df_2016)):
    asjc_list = df_2016.asjc.iloc[i]
    for asjc in asjc_list:
        dict_2016[asjc].append(df_2016.citation.iloc[i])

# deepcopy the dict to avoid the reference
dict_2016 = deepcopy(dict_2016)


## asjc
# compute the h-index
dict_2016 = h_index_from_dict(dict_2016)

## qs faculty
fac_dict_2016 = {fac:[] for fac in set(dict_qs_faculty.values())}

for key, val in dict_2016.items():
    if int(key) in dict_qs_faculty.keys():
        fac_dict_2016[dict_qs_faculty[int(key)]] += (val)


# compute the h-index for each fac_dict_2016 keys
fac_dict_2016 = h_index_from_dict(fac_dict_2016)


## qs subject
subj_dict_2016 = {subj:[] for subj in set(dict_qs_subject.values())}

for key, val in dict_2016.items():
    if int(key) in dict_qs_subject.keys():
        subj_dict_2016[dict_qs_subject[int(key)]] += (val)

subj_dict_2016 = h_index_from_dict(subj_dict_2016)


## save as json
# merge the dict
h_index_2016 = {'H-Index by ASJC': dict_2016, 'H-Index by QS Faculty': fac_dict_2016, 'H-Index by QS Subject': subj_dict_2016}

# save as json
with open('h_index_2016.json', 'w') as fp:
    json.dump(h_index_2016, fp)
    
### 2017-2021
df_2017 = temp[temp.year >= '2017']
df_2017 = df_2017[df_2017.year <= '2021']

list_2017 = df_2017.asjc.to_list()
set_2017 = set([item for sublist in list_2017 for item in sublist])

dict_2017 = {asjc:[] for asjc in set_2017}

# fill the dict by the citation of each ASJC from the dataframe
for i in range(len(df_2017)):
    asjc_list = df_2017.asjc.iloc[i]
    for asjc in asjc_list:
        dict_2017[asjc].append(df_2017.citation.iloc[i])

# deepcopy the dict to avoid the reference
dict_2017 = deepcopy(dict_2017)

## asjc
# compute the h-index
dict_2017 = h_index_from_dict(dict_2017)


## qs faculty
fac_dict_2017 = {fac:[] for fac in set(dict_qs_faculty.values())}

for key, val in dict_2017.items():
    if int(key) in dict_qs_faculty.keys():
        fac_dict_2017[dict_qs_faculty[int(key)]] += (val)

# compute the h-index for each fac_dict_2016 keys
fac_dict_2017 = h_index_from_dict(fac_dict_2017)


## qs subject
subj_dict_2017 = {subj:[] for subj in set(dict_qs_subject.values())}

for key, val in dict_2017.items():
    if int(key) in dict_qs_subject.keys():
        subj_dict_2017[dict_qs_subject[int(key)]] += (val)


subj_dict_2017 = h_index_from_dict(subj_dict_2017)


## save as json
# merge the dict
h_index_2017 = {'H-Index by ASJC': dict_2017, 'H-Index by QS Faculty': fac_dict_2017, 'H-Index by QS Subject': subj_dict_2017}

# save as json
with open('h_index_2017.json', 'w') as fp:
    json.dump(h_index_2017, fp)
    





