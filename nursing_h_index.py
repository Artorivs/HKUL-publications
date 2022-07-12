import pandas as pd
import numpy as np
from hindexpy.HIndex import h_index_from_dict
from hindexpy.CleanData import convert_setOfString_to_list, convert_df_to_dict, convert_column_datetime, convert_pipeSeperatedString_to_list
from hindexpy.ProcessData import filter_sort_and_drop_duplicates, filter_useless_column, filter_year_range, categorise_citation_by_map

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

## copy a tempory dataframe to avoid polluting the original dataframe
temp = df.copy()

## set values in cover_date as datetime
temp = convert_column_datetime(temp, 'cover_date')
temp = filter_useless_column(temp, ['full_name', 'cover_date', 'citation_count', 'asjcs'], dropna='any')
temp = convert_setOfString_to_list(temp, 'asjcs')

## only keep the ones from specific year
temp2016, year2016 = filter_year_range(temp, 'cover_date', 2016, 2020) # from 2016 to 2020
temp2017, year2017 = filter_year_range(temp, 'cover_date', 2017, 2021) # from 2017 to 2021

## university-level data
df_uni = filter_useless_column(df_uni, ['Title', 'Year', 'Citations', 'All Science Journal Classification (ASJC) code'], dropna=True)
df_uni = convert_pipeSeperatedString_to_list(df_uni, 'All Science Journal Classification (ASJC) code')
df_uni.columns = ['title', 'year', 'citations', 'asjc']
df_uni = convert_column_datetime(df_uni, 'year', '%Y')

temp_uni_2016, _ = filter_year_range(df_uni, 'year', 2016, 2020)
temp_uni_2017, _ = filter_year_range(df_uni, 'year', 2017, 2021)

### categorise and calculate h-index
## department average
dict_average_general_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count', option='map', map_object=asjc_map_dict)
dict_average_subject_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count', option='map', map_object=subj_map_dict)
dict_average_faculty_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count', option='map', map_object=facu_map_dict)

dict_average_general_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count', option='map', map_object=asjc_map_dict)
dict_average_subject_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count', option='map', map_object=subj_map_dict)
dict_average_faculty_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count', option='map', map_object=facu_map_dict)

dict_average_general_2016 = h_index_from_dict(dict_average_general_2016)
dict_average_subject_2016 = h_index_from_dict(dict_average_subject_2016)
dict_average_faculty_2016 = h_index_from_dict(dict_average_faculty_2016)

dict_average_general_2017 = h_index_from_dict(dict_average_general_2017)
dict_average_subject_2017 = h_index_from_dict(dict_average_subject_2017)
dict_average_faculty_2017 = h_index_from_dict(dict_average_faculty_2017)

## asjc
dict_general_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=asjc_map_dict)
dict_general_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=asjc_map_dict)

for author in dict_general_2016: dict_general_2016[author] = h_index_from_dict(dict_general_2016[author])
for author in dict_general_2017: dict_general_2017[author] = h_index_from_dict(dict_general_2017[author])

## qs-faculty
dict_faculty_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=facu_map_dict)
dict_faculty_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=facu_map_dict)

for author in dict_faculty_2016: dict_faculty_2016[author] = h_index_from_dict(dict_faculty_2016[author])
for author in dict_faculty_2017: dict_faculty_2017[author] = h_index_from_dict(dict_faculty_2017[author])

## qs-subject
dict_subject_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=subj_map_dict)
dict_subject_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=subj_map_dict)

for author in dict_subject_2016: dict_subject_2016[author] = h_index_from_dict(dict_subject_2016[author])
for author in dict_subject_2017: dict_subject_2017[author] = h_index_from_dict(dict_subject_2017[author])

### data postprocessing
## merge the data: add department average into each dict
dict_general_2016['AVERAGE'] = dict_average_general_2016
dict_general_2017['AVERAGE'] = dict_average_general_2017

dict_subject_2016['AVERAGE'] = dict_average_subject_2016
dict_subject_2017['AVERAGE'] = dict_average_subject_2017

dict_faculty_2016['AVERAGE'] = dict_average_faculty_2016
dict_faculty_2017['AVERAGE'] = dict_average_faculty_2017

## convert to a dataframe
h_index_general_2016 = pd.DataFrame.from_dict(dict_general_2016).T
h_index_general_2017 = pd.DataFrame.from_dict(dict_general_2017).T

h_index_faculty_2016 = pd.DataFrame.from_dict(dict_faculty_2016).T
h_index_faculty_2017 = pd.DataFrame.from_dict(dict_faculty_2017).T

h_index_subject_2016 = pd.DataFrame.from_dict(dict_subject_2016).T
h_index_subject_2017 = pd.DataFrame.from_dict(dict_subject_2017).T

## save the data
h_index_general_2016.to_csv('./dataset/processed/nursing_h-index_by_asjc_2016-2020.tsv', sep='\t')
h_index_general_2017.to_csv('./dataset/processed/nursing_h-index_by_asjc_2017-2021.tsv', sep='\t')

h_index_faculty_2016.to_csv('./dataset/processed/nursing_h-index_by_qs_faculty_2016-2020.tsv', sep='\t')
h_index_faculty_2017.to_csv('./dataset/processed/nursing_h-index_by_qs_faculty_2017-2021.tsv', sep='\t')

h_index_subject_2016.to_csv('./dataset/processed/nursing_h-index_by_qs_subject_2016-2020.tsv', sep='\t')
h_index_subject_2017.to_csv('./dataset/processed/nursing_h-index_by_qs_subject_2017-2021.tsv', sep='\t')