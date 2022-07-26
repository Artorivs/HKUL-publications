import pandas as pd
import numpy as np
from hindexpy.HIndex import h_index_from_dict
from hindexpy.CleanData import convert_setOfString_to_list, convert_df_to_dict, convert_column_datetime, convert_pipeSeperatedString_to_list
from hindexpy.ProcessData import filter_sort_and_drop_duplicates, filter_useless_column, filter_year_range, categorise_citation_by_map

### read the data
df = pd.read_csv('./hkul-publications/dataset/nursing/nursing.csv') # major dataset
# df_uni = pd.read_csv('./hkul-publications/dataset/university/HKU2016-2021.csv') # university dataset

xlsx = pd.ExcelFile('./hkul-publications/dataset/ASJC to QS_Sept2020.xlsx')
subj_map = pd.read_excel(xlsx, 'ASJC - QS (Subj Map)', dtype={'ASJC code': str})
facu_map = pd.read_excel(xlsx, 'ASJC - QS faculty areas (Top le', dtype={'ASJC code': str})

asjc_map = pd.read_excel('./hkul-publications/dataset/ASJC_CODE_NAME.xlsx', dtype={'Code': str})

### prepare map objects
subj_map = filter_sort_and_drop_duplicates(subj_map, 'ASJC code', drop_dup=True)
facu_map = filter_sort_and_drop_duplicates(facu_map, 'ASJC code', drop_dup=True)
asjc_map = filter_sort_and_drop_duplicates(asjc_map, 'Code', drop_dup=True)

subj_map_dict = convert_df_to_dict(subj_map, 'ASJC code', 'QS subject name')
facu_map_dict = convert_df_to_dict(facu_map, 'ASJC code', 'QS faculty area name')
asjc_map_dict = convert_df_to_dict(asjc_map, 'Code', 'Field')

### filter if the articles from school-level are in university-level, so as to ensure all are from HKU
## copy a tempory dataframe to avoid polluting the original dataframe
temp = df.copy()

## set values in cover_date as datetime
temp = convert_column_datetime(temp, 'cover_date')
temp = filter_useless_column(temp, ['full_name', 'scopus_author_id', 'title', 'cover_date', 'citation_count', 'asjcs'], dropna='any')
temp = convert_setOfString_to_list(temp, 'asjcs')

temp, _ = filter_year_range(temp, 'cover_date', 2016, 2021)

## university-level data
# temp_uni = df_uni.copy()

# temp_uni = filter_useless_column(temp_uni, ['Title', 'Scopus Author Ids', 'Year', 'Citations', 'All Science Journal Classification (ASJC) code'], dropna=True)
# temp_uni = convert_pipeSeperatedString_to_list(temp_uni, 'All Science Journal Classification (ASJC) code')
# temp_uni = convert_pipeSeperatedString_to_list(temp_uni, 'Scopus Author Ids')
# temp_uni.columns = ['title', 'scopus', 'year', 'citations', 'asjc']
# temp_uni = convert_column_datetime(temp_uni, 'year', '%Y')

# temp_uni, _ = filter_year_range(temp_uni, 'year', 2016, 2021)

## ~~trim the dataframe to only include the articles from HKU~~
## No Longer Needed: to be fair to the newly comed scholars
# list_university_title = temp_uni['title'].unique()

# for i in range(len(temp)):
#     if temp.loc[i, 'title'] not in list_university_title:
#         # delete the row
#         temp.drop(i, inplace=True)

### clean the data
temp = filter_useless_column(temp, ['full_name', 'title', 'cover_date', 'citation_count', 'asjcs'], dropna='any')
temp.reset_index(drop=True, inplace=True)

## only keep the ones from specific year
temp2016, year2016 = filter_year_range(temp, 'cover_date', 2016, 2020) # from 2016 to 2020
temp2017, year2017 = filter_year_range(temp, 'cover_date', 2017, 2021) # from 2017 to 2021

# temp_uni_2016, _ = filter_year_range(temp_uni, 'year', 2016, 2020)
# temp_uni_2017, _ = filter_year_range(temp_uni, 'year', 2017, 2021)

### processing data
## average of nursing-school-level data
## average is useless in h-index computation
# categorise by map
# dict_average_asjc_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count', option='map', map_object=asjc_map_dict)
# dict_average_subject_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count', option='map', map_object=subj_map_dict)
# dict_average_faculty_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count', option='map', map_object=facu_map_dict)

# dict_average_asjc_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count', option='map', map_object=asjc_map_dict)
# dict_average_subject_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count', option='map', map_object=subj_map_dict)
# dict_average_faculty_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count', option='map', map_object=facu_map_dict)

# compute h-index
# h_index_average_asjc_2016 = h_index_from_dict(dict_average_asjc_2016)
# h_index_average_subject_2016 = h_index_from_dict(dict_average_subject_2016)
# h_index_average_faculty_2016 = h_index_from_dict(dict_average_faculty_2016)

# h_index_average_asjc_2017 = h_index_from_dict(dict_average_asjc_2017)
# h_index_average_subject_2017 = h_index_from_dict(dict_average_subject_2017)
# h_index_average_faculty_2017 = h_index_from_dict(dict_average_faculty_2017)

## university-level data
# categorise by map
# dict_uni_average_asjc_2016 = categorise_citation_by_map(temp_uni_2016, 'asjc', 'citations', option='map', map_object=asjc_map_dict)
# dict_uni_average_subject_2016 = categorise_citation_by_map(temp_uni_2016, 'asjc', 'citations', option='map', map_object=subj_map_dict)
# dict_uni_average_faculty_2016 = categorise_citation_by_map(temp_uni_2016, 'asjc', 'citations', option='map', map_object=facu_map_dict)

# dict_uni_average_asjc_2017 = categorise_citation_by_map(temp_uni_2017, 'asjc', 'citations', option='map', map_object=asjc_map_dict)
# dict_uni_average_subject_2017 = categorise_citation_by_map(temp_uni_2017, 'asjc', 'citations', option='map', map_object=subj_map_dict)
# dict_uni_average_faculty_2017 = categorise_citation_by_map(temp_uni_2017, 'asjc', 'citations', option='map', map_object=facu_map_dict)

# compute h-index
# h_index_uni_average_asjc_2016 = h_index_from_dict(dict_uni_average_asjc_2016)
# h_index_uni_average_subject_2016 = h_index_from_dict(dict_uni_average_subject_2016)
# h_index_uni_average_faculty_2016 = h_index_from_dict(dict_uni_average_faculty_2016)

# h_index_uni_average_asjc_2017 = h_index_from_dict(dict_uni_average_asjc_2017)
# h_index_uni_average_subject_2017 = h_index_from_dict(dict_uni_average_subject_2017)
# h_index_uni_average_faculty_2017 = h_index_from_dict(dict_uni_average_faculty_2017)

### ensure h-index of is the same in mapping subjects as the one of the school-level
## a function to remove keys of a dictionary which is not in the second dictionary
# def trim_keys(dict_1, dict_2):
#     target_keylist = list(dict_1.keys())
#     sample_keylist = list(dict_2.keys())
    
#     for key in target_keylist:
#         if key not in sample_keylist:
#             del dict_1[key]
            
#     return dict_1

## trim the keys from the university-level data which is not in the asjc map data
# h_index_uni_average_faculty_2016 = trim_keys(h_index_uni_average_faculty_2016, h_index_average_faculty_2016)
# h_index_uni_average_faculty_2017 = trim_keys(h_index_uni_average_faculty_2017, h_index_average_faculty_2017)
# h_index_uni_average_subject_2016 = trim_keys(h_index_uni_average_subject_2016, h_index_average_subject_2016)
# h_index_uni_average_subject_2017 = trim_keys(h_index_uni_average_subject_2017, h_index_average_subject_2017)
# h_index_uni_average_asjc_2016 = trim_keys(h_index_uni_average_asjc_2016, h_index_average_asjc_2016)
# h_index_uni_average_asjc_2017 = trim_keys(h_index_uni_average_asjc_2017, h_index_average_asjc_2017)

### compute the h-index of every scholars in the school
## asjc
dict_asjc_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=asjc_map_dict)
dict_asjc_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=asjc_map_dict)

for author in dict_asjc_2016: dict_asjc_2016[author] = h_index_from_dict(dict_asjc_2016[author])
for author in dict_asjc_2017: dict_asjc_2017[author] = h_index_from_dict(dict_asjc_2017[author])

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
## average is useless in h-index
# dict_asjc_2016['AVERAGE (SCHOOL)'] = dict_average_asjc_2016
# dict_asjc_2017['AVERAGE (SCHOOL)'] = dict_average_asjc_2017
# dict_subject_2016['AVERAGE (SCHOOL)'] = dict_average_subject_2016
# dict_subject_2017['AVERAGE (SCHOOL)'] = dict_average_subject_2017
# dict_faculty_2016['AVERAGE (SCHOOL)'] = dict_average_faculty_2016
# dict_faculty_2017['AVERAGE (SCHOOL)'] = dict_average_faculty_2017

# dict_asjc_2016['AVERAGE (UNIVERSITY)'] = dict_uni_average_asjc_2016
# dict_asjc_2017['AVERAGE (UNIVERSITY)'] = dict_uni_average_asjc_2017
# dict_subject_2016['AVERAGE (UNIVERSITY)'] = dict_uni_average_subject_2016
# dict_subject_2017['AVERAGE (UNIVERSITY)'] = dict_uni_average_subject_2017
# dict_faculty_2016['AVERAGE (UNIVERSITY)'] = dict_uni_average_faculty_2016
# dict_faculty_2017['AVERAGE (UNIVERSITY)'] = dict_uni_average_faculty_2017

## convert to a dataframe
h_index_general_2016 = pd.DataFrame.from_dict(dict_asjc_2016).T
h_index_general_2017 = pd.DataFrame.from_dict(dict_asjc_2017).T

h_index_faculty_2016 = pd.DataFrame.from_dict(dict_faculty_2016).T
h_index_faculty_2017 = pd.DataFrame.from_dict(dict_faculty_2017).T

h_index_subject_2016 = pd.DataFrame.from_dict(dict_subject_2016).T
h_index_subject_2017 = pd.DataFrame.from_dict(dict_subject_2017).T

## save the data
h_index_general_2016.to_csv('./hkul-publications/processed/nursing_h-index_by_asjc_2016-2020.tsv', sep='\t')
h_index_general_2017.to_csv('./hkul-publications/processed/nursing_h-index_by_asjc_2017-2021.tsv', sep='\t')

h_index_faculty_2016.to_csv('./hkul-publications/processed/nursing_h-index_by_qs_faculty_2016-2020.tsv', sep='\t')
h_index_faculty_2017.to_csv('./hkul-publications/processed/nursing_h-index_by_qs_faculty_2017-2021.tsv', sep='\t')

h_index_subject_2016.to_csv('./hkul-publications/processed/nursing_h-index_by_qs_subject_2016-2020.tsv', sep='\t')
h_index_subject_2017.to_csv('./hkul-publications/processed/nursing_h-index_by_qs_subject_2017-2021.tsv', sep='\t')
