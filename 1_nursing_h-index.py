import numpy as np
import pandas as pd
from hindex import SubjectCitation, h_index_by_subject, h_index, dict_to_list, df_to_dict, h_index_from_series_of_dict

### Load data
df = pd.read_csv('./dataset/nursing.csv') # major dataset
xls = pd.ExcelFile('./dataset/ASJC to QS_Sept2020.xlsx')
subj_map = pd.read_excel(xls, 'ASJC - QS (Subj Map)', dtype={'ASJC code': str})
fac_area = pd.read_excel(xls, 'ASJC - QS faculty areas (Top le', dtype={'ASJC code': str}) # dtype={'ASJC code': str}

# rearrange the dataframe by the asjc code
subj_map = subj_map.sort_values(by='ASJC code').reset_index(drop=True)
fac_area = fac_area.sort_values(by='ASJC code').reset_index(drop=True)

# drop the duplicated rows
fac_area = fac_area.drop_duplicates(subset='ASJC code', keep='first')

lack_code_list = []
for i in range(len(fac_area)):
    if fac_area.iloc[i]['ASJC code'] not in subj_map['ASJC code'].unique():
        lack_code_list.append(fac_area.iloc[i]['ASJC code'])

subj_map_dict = df_to_dict(subj_map, 'ASJC code', 'QS subject name')
fac_map_dict = df_to_dict(fac_area, 'ASJC code', 'QS faculty area name')

## copy a tempory dataframe
temp = df.copy()


# set values in cover_date as datetime
temp['cover_date'] = pd.to_datetime(temp['cover_date'])
# filter datetime values: only keep the ones from 2016 to 2020
temp = temp[temp['cover_date'].dt.year >= 2017]
temp = temp[temp['cover_date'].dt.year <= 2021]
temp.reset_index(drop=True, inplace=True)

# make a list of year range
year_set = set()
for i in range(len(temp)):
    year_set.add(temp.iloc[i]['cover_date'].year)


str_of_set_to_list = lambda x: str(x).replace('{', '').replace('}', '').split(',')
temp['asjcs'] = temp['asjcs'].apply(str_of_set_to_list)


# list unique values in column 'handle'
handle_list = temp['handle'].unique()

h_index_general_dict = {}
# compute the h-index for each author
for handle in handle_list:
    # get the list of citations for the author
    citations = temp[temp['handle'] == handle]['citation_count'].tolist()
    # compute the h-index
    h_index_general = h_index(citations)
    # store the general h-index 
    h_index_general_dict[handle] = h_index_general
    

### stage 2: asjc h-index


author_data_dict = {} # {author_handle: {'subject_id': [citation_count1, citation_count2, ...]}}

## compute the h-index with the consideration of asjcs
for handle in handle_list:
    # get the list of asjcs for the author
    asjcs = temp[temp['handle'] == handle]['asjcs'].tolist()
    # create a SubjectCitation object
    author_subject_citation = SubjectCitation()
    index = 0
    # for each asjcs, put the citation count into the SubjectCitation object
    for subjects in asjcs:
        for subject in subjects:
            author_subject_citation.put(subject, temp.iloc[index]['citation_count'])
        index += 1
    
    author_data_dict[handle] = author_subject_citation.to_dict()


for handle in handle_list:
    for subject, citation_count in author_data_dict[handle].items():
        # compute the h-index with the consideration of asjcs
        h_index_value = h_index(citation_count)
        author_data_dict[handle][subject] = h_index_value


### stage 3: QS Faculty H-index
temp1 = temp.copy()


# temp1 append a new column 'faculty'
temp1['faculty'] = np.nan


total_temp_lst = []
for i in range(len(temp1)):
    temp_set = set()
    for j in range(len(temp1['asjcs'][i])):
        if temp1['asjcs'][i][j] in fac_map_dict:
            x = fac_map_dict[temp1.iloc[i]['asjcs'][j]]
            temp_set.add(x)
    
    if temp_set == set():
        temp_set.add('NaN')
        
    total_temp_lst.append(temp_set)


# merge pd.Series(total_temp_lst) to temp1
temp1['faculty'] = pd.Series(total_temp_lst)


qs_faculty_list = h_index_by_subject(temp1, handle_list, 'handle', 'citation_count', 'faculty')


h_index_qs_faculty_list = h_index_from_series_of_dict(qs_faculty_list, dropna=True)


### stage 4: QS subject H-index
temp1['subject'] = np.nan

total_temp_lst = []
for i in range(len(temp1)):
    temp_set = set()
    for j in range(len(temp1['asjcs'][i])):
        if temp1['asjcs'][i][j] in subj_map_dict:
            x = subj_map_dict[temp1.iloc[i]['asjcs'][j]]
            temp_set.add(x)
    
    if temp_set == set():
        temp_set.add('NaN')
        
    total_temp_lst.append(temp_set)

# merge pd.Series(total_temp_lst) to temp1
temp1['subject'] = pd.Series(total_temp_lst)

qs_subject_list = h_index_by_subject(temp1, handle_list, 'handle', 'citation_count', 'subject')

h_index_qs_subject_list = h_index_from_series_of_dict(qs_subject_list, dropna=True)

## merging all lists into a dataframe
h_index_general_list = pd.Series(dict_to_list(handle_list, h_index_general_dict))
h_index_by_subject_list = pd.Series(dict_to_list(handle_list, author_data_dict))

years = str(min(year_set)) + '-' + str(max(year_set))

# merge series: h_index_general_list, h_index_by_subject_list, h_index_qs_faculty_list, h_index_qs_subject_list
author_df = pd.DataFrame(columns=['handle', 'year', 'h_index_general', 'h_index_by_asjc', 'h_index_by_qs_faculty', 'h_index_by_qs_subject'])
author_df['handle'] = handle_list
author_df['year'] = years
author_df['h_index_general'] = h_index_general_list
author_df['h_index_by_asjc'] = h_index_by_subject_list
author_df['h_index_by_qs_faculty'] = h_index_qs_faculty_list
author_df['h_index_by_qs_subject'] = h_index_qs_subject_list

## save the dataframe to csv
author_df.to_csv('./dataset/nursing_h_index (2017-2021).csv', index=False)
