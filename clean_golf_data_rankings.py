"""
Cleans the Data Golf rankings adding the next PGA Tour event to match with archvied events
"""


import pandas as pd
import glob

path='data/archived_raw_rankings/dg_ranks_'

for file_name in glob.glob(path+'*.csv'):
    year=file_name[-8:-4]
    event_id=file_name.split('_')[-2].split('=')[1]
    year=file_name.split('ranks_')[1].split('_')[0]
    date_year=file_name.split('ranks_')[1].split('_')[0]
    month=file_name.split('ranks_')[1].split('_')[1]
    day=file_name.split('ranks_')[1].split('_')[2]
    date=date_year+'-'+month+'-'+day
    
    file=pd.read_csv(file_name)
    file['date']=date
    file['year']=year
    file['event_id']=event_id
    file=file[['date','year','event_id','player_name','sample_size','primary_tour','dg_rank','dg_change','owgr_rank','owgr_change','dg_index']]
    file.to_csv('data/cleaned_rankings/dg_ranks_'+date_year+'_'+month+'_'+day+'_event='+event_id+'_year='+year+'.csv',index=False)
    
    
for file_name in glob.glob(path+'*event=7_year=2020.csv'):
    print(file_name)