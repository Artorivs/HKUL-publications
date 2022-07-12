import pandas as pd
import numpy as np
from hindexpy.CleanData import convert_setOfString_to_list, convert_df_to_dict, convert_column_datetime
from hindexpy.ProcessData import filter_sort_and_drop_duplicates, filter_useless_column, filter_year_range

### read the data
df = pd.read_csv('./dataset/nursing/nursing.csv') # major dataset
df_uni = pd.read_csv('./dataset/university/HKU2016-2021.csv') # university dataset

xlsx = pd.ExcelFile('./dataset/ASJC to QS_Sept2020.xlsx')
subj_map = pd.read_excel(xlsx, 
                         'ASJC - QS (Subj Map)', 
                         dtype={'ASJC code': str})
facu_map = pd.read_excel(xlsx, 
                         'ASJC - QS faculty areas (Top le', 
                         dtype={'ASJC code': str})

asjc_map = pd.read_excel('./dataset/ASJC_CODE_NAME.xlsx',
                          dtype={'Code': str})

## clean the data
subj_map = filter_sort_and_drop_duplicates(subj_map, 'ASJC code', drop_dup=True)
facu_map = filter_sort_and_drop_duplicates(facu_map, 'ASJC code', drop_dup=True)
asjc_map = filter_sort_and_drop_duplicates(asjc_map, 'Code', drop_dup=True)

subj_map_dict = convert_df_to_dict(subj_map, 'ASJC code', 'QS subject name')
facu_map_dict = convert_df_to_dict(facu_map, 'ASJC code', 'QS faculty area name')
asjc_map_dict = convert_df_to_dict(asjc_map, 'Code', 'Field')

## copy a tempory dataframe
temp = df.copy()

## set values in cover_date as datetime
temp = convert_column_datetime(temp, 'cover_date')
temp = filter_useless_column(temp, ['full_name', 'cover_date', 'citation_count', 'asjcs'], dropna='any')
temp = convert_setOfString_to_list(temp, 'asjcs')

## only keep the ones from specific year
temp2016, year2016 = filter_year_range(temp, 'cover_date', 2016, 2020) # from 2016 to 2020
temp2017, year2017 = filter_year_range(temp, 'cover_date', 2017, 2021) # from 2017 to 2021


### a function for flattening a list of lists
def _flatten_list(l):
    for i in l:
        if isinstance(i, list):
            yield from _flatten_list(i)
        else:
            yield i
  
## asjc - author 
# 2016          
temp_asjc_author_2016 = pd.DataFrame(columns=['asjc', 'full_name', 'count'])
for i in range(len(temp2016)):
    asjc_list = list(_flatten_list(temp2016.iloc[i]['asjcs']))
    author_name = temp2016.iloc[i]['full_name']
    for j in range(len(asjc_list)):
        if asjc_list[j] in asjc_map_dict:
            temp_asjc_author_2016.loc[len(temp_asjc_author_2016)] = [asjc_map_dict[asjc_list[j]], author_name, 1]

temp_asjc_author_2016 = temp_asjc_author_2016.groupby(['asjc', 'full_name']).sum().reset_index()

# 2017
temp_asjc_author_2017 = pd.DataFrame(columns=['asjc', 'full_name', 'count'])
for i in range(len(temp2017)):
    asjc_list = list(_flatten_list(temp2017.iloc[i]['asjcs']))
    author_name = temp2017.iloc[i]['full_name']
    for j in range(len(asjc_list)):
        if asjc_list[j] in asjc_map_dict:
            temp_asjc_author_2017.loc[len(temp_asjc_author_2017)] = [asjc_map_dict[asjc_list[j]], author_name, 1]

temp_asjc_author_2017 = temp_asjc_author_2017.groupby(['asjc', 'full_name']).sum().reset_index()

## qs_subject - author
# 2016
temp_subj_author_2016 = pd.DataFrame(columns=['qs_subject', 'full_name', 'count'])
for i in range(len(temp2016)):
    asjc_list = list(_flatten_list(temp2016.iloc[i]['asjcs']))
    author_name = temp2016.iloc[i]['full_name']
    for j in range(len(asjc_list)):
        if asjc_list[j] in subj_map_dict:
            temp_subj_author_2016.loc[len(temp_subj_author_2016)] = [subj_map_dict[asjc_list[j]], author_name, 1]

temp_subj_author_2016 = temp_subj_author_2016.groupby(['qs_subject', 'full_name']).sum().reset_index()

# 2017
temp_subj_author_2017 = pd.DataFrame(columns=['qs_subject', 'full_name', 'count'])
for i in range(len(temp2017)):
    asjc_list = list(_flatten_list(temp2017.iloc[i]['asjcs']))
    author_name = temp2017.iloc[i]['full_name']
    for j in range(len(asjc_list)):
        if asjc_list[j] in subj_map_dict:
            temp_subj_author_2017.loc[len(temp_subj_author_2017)] = [subj_map_dict[asjc_list[j]], author_name, 1]

temp_subj_author_2017 = temp_subj_author_2017.groupby(['qs_subject', 'full_name']).sum().reset_index()

## qs_faculty - author 
# 2016
temp_facu_author_2016 = pd.DataFrame(columns=['qs_faculty', 'full_name', 'count'])
for i in range(len(temp2016)):
    asjc_list = list(_flatten_list(temp2016.iloc[i]['asjcs']))
    author_name = temp2016.iloc[i]['full_name']
    for j in range(len(asjc_list)):
        if asjc_list[j] in facu_map_dict:
            temp_facu_author_2016.loc[len(temp_facu_author_2016)] = [facu_map_dict[asjc_list[j]], author_name, 1]

temp_facu_author_2016 = temp_facu_author_2016.groupby(['qs_faculty', 'full_name']).sum().reset_index()

# 2017
temp_facu_author_2017 = pd.DataFrame(columns=['qs_faculty', 'full_name', 'count'])
for i in range(len(temp2017)):
    asjc_list = list(_flatten_list(temp2017.iloc[i]['asjcs']))
    author_name = temp2017.iloc[i]['full_name']
    for j in range(len(asjc_list)):
        if asjc_list[j] in facu_map_dict:
            temp_facu_author_2017.loc[len(temp_facu_author_2017)] = [facu_map_dict[asjc_list[j]], author_name, 1]

temp_facu_author_2017 = temp_facu_author_2017.groupby(['qs_faculty', 'full_name']).sum().reset_index()

## asjc - qs_subject
# 2016
temp_asjc_subj_2016 = pd.DataFrame(columns=['asjc', 'qs_subject', 'count'])

for i in range(len(temp2016)):
    asjc_list = list(_flatten_list(temp2016.iloc[i]['asjcs']))
    for j in range(len(asjc_list)):
        if asjc_list[j] in asjc_map_dict and asjc_list[j] in subj_map_dict:
            temp_asjc_subj_2016.loc[len(temp_asjc_subj_2016)] = [asjc_map_dict[asjc_list[j]], subj_map_dict[asjc_list[j]], 1]

temp_asjc_subj_2016 = temp_asjc_subj_2016.groupby(['asjc', 'qs_subject']).sum().reset_index()

# 2017
temp_asjc_subj_2017 = pd.DataFrame(columns=['asjc', 'qs_subject', 'count'])

for i in range(len(temp2017)):
    asjc_list = list(_flatten_list(temp2017.iloc[i]['asjcs']))
    for j in range(len(asjc_list)):
        if asjc_list[j] in asjc_map_dict and asjc_list[j] in subj_map_dict:
            temp_asjc_subj_2017.loc[len(temp_asjc_subj_2017)] = [asjc_map_dict[asjc_list[j]], subj_map_dict[asjc_list[j]], 1]
            
temp_asjc_subj_2017 = temp_asjc_subj_2017.groupby(['asjc', 'qs_subject']).sum().reset_index()

## qs_subject - qs_faculty
# 2016
temp_subj_facu_2016 = pd.DataFrame(columns=['qs_subject', 'qs_faculty', 'count'])

for i in range(len(temp2016)):
    asjc_list = list(_flatten_list(temp2016.iloc[i]['asjcs']))
    for j in range(len(asjc_list)):
        if asjc_list[j] in subj_map_dict and asjc_list[j] in facu_map_dict:
            temp_subj_facu_2016.loc[len(temp_subj_facu_2016)] = [subj_map_dict[asjc_list[j]], facu_map_dict[asjc_list[j]], 1]

temp_subj_facu_2016 = temp_subj_facu_2016.groupby(['qs_subject', 'qs_faculty']).sum().reset_index()

# 2017
temp_subj_facu_2017 = pd.DataFrame(columns=['qs_subject', 'qs_faculty', 'count'])

for i in range(len(temp2017)):
    asjc_list = list(_flatten_list(temp2017.iloc[i]['asjcs']))
    for j in range(len(asjc_list)):
        if asjc_list[j] in subj_map_dict and asjc_list[j] in facu_map_dict:
            temp_subj_facu_2017.loc[len(temp_subj_facu_2017)] = [subj_map_dict[asjc_list[j]], facu_map_dict[asjc_list[j]], 1]
            
temp_subj_facu_2017 = temp_subj_facu_2017.groupby(['qs_subject', 'qs_faculty']).sum().reset_index()

# save to tsv
temp_asjc_author_2016.to_csv('./dataset/processed/nursing_bipartile_asjc_to_author_2016-2020.tsv', sep='\t')
temp_asjc_author_2017.to_csv('./dataset/processed/nursing_bipartile_asjc_to_author_2017-2021.tsv', sep='\t')

temp_subj_author_2016.to_csv('./dataset/processed/nursing_bipartile_qs_subject_to_author_2016-2020.tsv', sep='\t')
temp_subj_author_2017.to_csv('./dataset/processed/nursing_bipartile_qs_subject_to_author_2017-2021.tsv', sep='\t')

temp_facu_author_2016.to_csv('./dataset/processed/nursing_bipartile_qs_faculty_to_author_2016-2020.tsv', sep='\t')
temp_facu_author_2017.to_csv('./dataset/processed/nursing_bipartile_qs_faculty_to_author_2017-2021.tsv', sep='\t')

temp_asjc_subj_2016.to_csv('./dataset/processed/nursing_bipartile_asjc_to_qs_subject_2016-2020.tsv', sep='\t')
temp_asjc_subj_2017.to_csv('./dataset/processed/nursing_bipartile_asjc_to_qs_subject_2017-2021.tsv', sep='\t')

temp_subj_facu_2016.to_csv('./dataset/processed/nursing_bipartile_qs_subject_to_qs_faculty_2016-2020.tsv', sep='\t')
temp_subj_facu_2017.to_csv('./dataset/processed/nursing_bipartile_qs_subject_to_qs_faculty_2017-2021.tsv', sep='\t')
