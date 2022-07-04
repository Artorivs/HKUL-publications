import pandas as pd
from hindexpy.HIndex import h_index_from_dict
from hindexpy.CleanData import convert_setofstring_to_list, convert_df_to_dict, convert_column_datetime
from hindexpy.ProcessData import filter_sort_and_drop_duplicates, filter_useless_column, filter_year_range, categorise_citation_by_map

### read the data
df = pd.read_csv('./dataset/nursing/nursing.csv') # major dataset

xlsx = pd.ExcelFile('./dataset/ASJC to QS_Sept2020.xlsx')
subj_map = pd.read_excel(xlsx, 
                         'ASJC - QS (Subj Map)', 
                         dtype={'ASJC code': str})
facu_map = pd.read_excel(xlsx, 
                         'ASJC - QS faculty areas (Top le', 
                         dtype={'ASJC code': str})

asjc_map = pd.read_excel('./dataset/ASJC_CODE_NAME.xlsx',
                          dtype={'Code': str})


# clean the data
subj_map = filter_sort_and_drop_duplicates(subj_map, 'ASJC code', drop_dup=True)
facu_map = filter_sort_and_drop_duplicates(facu_map, 'ASJC code', drop_dup=True)
asjc_map = filter_sort_and_drop_duplicates(asjc_map, 'Code', drop_dup=True)

subj_map_dict = convert_df_to_dict(subj_map, 'ASJC code', 'QS subject name')
facu_map_dict = convert_df_to_dict(facu_map, 'ASJC code', 'QS faculty area name')
asjc_map_dict = convert_df_to_dict(asjc_map, 'Code', 'Field')


## copy a tempory dataframe
temp = df.copy()

# set values in cover_date as datetime
temp = convert_column_datetime(temp, 'cover_date')


temp = filter_useless_column(temp, ['full_name', 'cover_date', 'citation_count', 'asjcs'], dropna='any')
temp = convert_setofstring_to_list(temp, 'asjcs')

# only keep the ones from specific year
temp2016, year2016 = filter_year_range(temp, 'cover_date', 2016, 2020) # from 2016 to 2020
temp2017, year2017 = filter_year_range(temp, 'cover_date', 2017, 2021) # from 2017 to 2021


dict_general_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=asjc_map_dict)
dict_general_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=asjc_map_dict)

for author in dict_general_2016: dict_general_2016[author] = h_index_from_dict(dict_general_2016[author])
for author in dict_general_2017: dict_general_2017[author] = h_index_from_dict(dict_general_2017[author])


dict_faculty_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=facu_map_dict)
dict_faculty_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=facu_map_dict)

for author in dict_faculty_2016: dict_faculty_2016[author] = h_index_from_dict(dict_faculty_2016[author])
for author in dict_faculty_2017: dict_faculty_2017[author] = h_index_from_dict(dict_faculty_2017[author])


dict_subject_2016 = categorise_citation_by_map(temp2016, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=subj_map_dict)
dict_subject_2017 = categorise_citation_by_map(temp2017, 'asjcs', 'citation_count',
                                               option='both', author_col='full_name', map_object=subj_map_dict)

for author in dict_subject_2016: dict_subject_2016[author] = h_index_from_dict(dict_subject_2016[author])
for author in dict_subject_2017: dict_subject_2017[author] = h_index_from_dict(dict_subject_2017[author])


# convert to a dataframe
h_index_general_2016 = pd.DataFrame.from_dict(dict_general_2016)
h_index_general_2017 = pd.DataFrame.from_dict(dict_general_2017)

h_index_faculty_2016 = pd.DataFrame.from_dict(dict_faculty_2016)
h_index_faculty_2017 = pd.DataFrame.from_dict(dict_faculty_2017)

h_index_subject_2016 = pd.DataFrame.from_dict(dict_subject_2016)
h_index_subject_2017 = pd.DataFrame.from_dict(dict_subject_2017)

h_index_general_2016.to_csv('./dataset/nursing_h-index_by_asjc_2016-2020.csv', sep='\t')
h_index_general_2017.to_csv('./dataset/nursing_h-index_by_asjc_2017-2021.csv', sep='\t')

h_index_faculty_2016.to_csv('./dataset/nursing_h-index_by_qs_faculty_2016-2020.csv', sep='\t')
h_index_faculty_2017.to_csv('./dataset/nursing_h-index_by_qs_faculty_2017-2021.csv', sep='\t')

h_index_subject_2016.to_csv('./dataset/nursing/h-index_by_qs_subject_2016-2020.tsv', sep='\t')
h_index_subject_2017.to_csv('./dataset/nursing/h-index_by_qs_subject_2017-2021.tsv', sep='\t')


