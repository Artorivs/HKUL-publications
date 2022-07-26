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

# clean the data
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
temp = filter_useless_column(temp, ['full_name', 'scopus_author_id', 'title', 'cover_date', 'citation_count', 'asjcs'], dropna='any')
temp = convert_setOfString_to_list(temp, 'asjcs')


temp, _ = filter_year_range(temp, 'cover_date', 2016, 2021)

## university-level data
temp_uni = df_uni.copy()

temp_uni = filter_useless_column(temp_uni, ['Title', 'Scopus Author Ids', 'Year', 'Citations', 'All Science Journal Classification (ASJC) code'], dropna=True)
temp_uni = convert_pipeSeperatedString_to_list(temp_uni, 'All Science Journal Classification (ASJC) code')
temp_uni = convert_pipeSeperatedString_to_list(temp_uni, 'Scopus Author Ids')
temp_uni.columns = ['title', 'scopus', 'year', 'citations', 'asjc']
temp_uni = convert_column_datetime(temp_uni, 'year', '%Y')

temp_uni, _ = filter_year_range(temp_uni, 'year', 2016, 2021)

list_university_title = temp_uni['title'].unique()

for i in range(len(temp)):
    if temp.loc[i, 'title'] not in list_university_title:
        # delete the row
        temp.drop(i, inplace=True)
        
temp = filter_useless_column(temp, ['full_name', 'title', 'cover_date', 'citation_count', 'asjcs'], dropna='any')
temp.reset_index(drop=True, inplace=True)
