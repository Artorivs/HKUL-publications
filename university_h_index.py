import numpy as np
import pandas as pd
from hindexpy.HIndex import h_index_from_dict
from hindexpy.CleanData import convert_pipeSeperatedString_to_list, convert_df_to_dict, convert_column_datetime, categorise_citation_by_map
from hindexpy.ProcessData import filter_sort_and_drop_duplicates, filter_useless_column, filter_year_range

# read the data
df = pd.read_csv('./dataset/university/HKU2016-2021.csv')


xlsx = pd.ExcelFile('./dataset/ASJC to QS_Sept2020.xlsx')
subj_map = pd.read_excel(xlsx, 
                         'ASJC - QS (Subj Map)', 
                         dtype={'ASJC code': str})
facu_map = pd.read_excel(xlsx, 
                         'ASJC - QS faculty areas (Top le', 
                         dtype={'ASJC code': str})

asjc_map = pd.read_excel('./dataset/ASJC_CODE_NAME.xlsx', 
                         usecols=['Code', 'Field'],
                         dtype={'Code': str})


# clean the data
subj_map = filter_sort_and_drop_duplicates(subj_map, 'ASJC code', drop_dup=True)
facu_map = filter_sort_and_drop_duplicates(facu_map, 'ASJC code', drop_dup=True)
asjc_map = filter_sort_and_drop_duplicates(asjc_map, 'Code', drop_dup=True)

subj_map_dict = convert_df_to_dict(subj_map, 'ASJC code', 'QS subject name')
facu_map_dict = convert_df_to_dict(facu_map, 'ASJC code', 'QS faculty area name')
asjc_map_dict = convert_df_to_dict(asjc_map, 'Code', 'Field')

df = filter_useless_column(df, ['Title', 'Year', 'Citations', 'All Science Journal Classification (ASJC) code'], dropna=True)

df = convert_pipeSeperatedString_to_list(df, 'All Science Journal Classification (ASJC) code')

df.columns = ['title', 'year', 'citations', 'asjc']

df = convert_column_datetime(df, 'year', '%Y')
temp2016, year2016 = filter_year_range(df, 'year', 2016, 2020)
temp2017, year2017 = filter_year_range(df, 'year', 2017, 2021)


## h-index by asjc
dict_general_2016 = categorise_citation_by_map(temp2016, 'asjc', 'citations', 
                                               option='map', map_object=asjc_map_dict)
dict_general_2017 = categorise_citation_by_map(temp2017, 'asjc', 'citations', 
                                               option='map', map_object=asjc_map_dict)

# compute the h-index
h_index_general_2016 = h_index_from_dict(dict_general_2016)
h_index_general_2017 = h_index_from_dict(dict_general_2017)

# convert to a dataframe
h_index_general_2016 = pd.DataFrame.from_dict(h_index_general_2016, orient='index')
h_index_general_2017 = pd.DataFrame.from_dict(h_index_general_2017, orient='index')

# merge the dataframes
h_index_general_data = pd.merge(h_index_general_2016, h_index_general_2017, how='outer', left_index=True, right_index=True).fillna('NaN')
h_index_general_data.columns = ['2016-2020', '2017-2021']

# # save as a tsv file
h_index_general_data.to_csv('./dataset/university_h-index_by_asjc.tsv', sep='\t')


## h-index by qs faculty
dict_faculty_2016 = categorise_citation_by_map(temp2016, 'asjc', 'citations', 
                                               option='map', map_object=facu_map_dict)
dict_faculty_2017 = categorise_citation_by_map(temp2017, 'asjc', 'citations', 
                                               option='map', map_object=facu_map_dict)

# compute the h-index
h_index_faculty_2016 = h_index_from_dict(dict_faculty_2016)
h_index_faculty_2017 = h_index_from_dict(dict_faculty_2017)

# convert to a dataframe
h_index_faculty_2016 = pd.DataFrame.from_dict(h_index_faculty_2016, orient='index', columns=['2016-2020'])
h_index_faculty_2017 = pd.DataFrame.from_dict(h_index_faculty_2017, orient='index', columns=['2017-2021'])

# merge the dataframes
h_index_faculty_data = pd.merge(h_index_faculty_2016, h_index_faculty_2017, how='outer', left_index=True, right_index=True).fillna('NaN')
h_index_faculty_data.columns = ['2016-2020', '2017-2021']

# save as a tsv file
h_index_faculty_data.to_csv('./dataset/university_h-index_by_qs_faculty.tsv', sep='\t')


## h-index by qs subject
dict_subject_2016 = categorise_citation_by_map(temp2016, 'asjc', 'citations', 
                                               option='map', map_object= subj_map_dict)
dict_subject_2017 = categorise_citation_by_map(temp2017, 'asjc', 'citations',
                                               option='map', map_object= subj_map_dict)

# compute the h-index
h_index_subject_2016 = h_index_from_dict(dict_subject_2016)
h_index_subject_2017 = h_index_from_dict(dict_subject_2017)

# convert to a dataframe
h_index_subject_2016 = pd.DataFrame.from_dict(h_index_subject_2016, orient='index', columns=[f'{min(year2016)}-{max(year2016)}'])
h_index_subject_2017 = pd.DataFrame.from_dict(h_index_subject_2017, orient='index', columns=[f'{min(year2017)}-{max(year2017)}'])

h_index_subject_data = pd.merge(h_index_subject_2016, h_index_subject_2017, how='outer', left_index=True, right_index=True).fillna('NaN')
h_index_subject_data.columns = ['2016-2020', '2017-2021']

# # save as a tsv file
h_index_subject_data.to_csv('./dataset/university_h-index_by_qs_subject.tsv', sep='\t')

