import pandas as pd
from datetime import datetime

###Date for saving
now=datetime.now()
today=now.strftime("%Y-%m-%d")

item_list=['win','top_5','top_10','top_20','make_cut','mc']

good_bets_list=[]
for item in item_list:
    euro_url='https://feeds.datagolf.com/betting-tools/outrights?tour=pga&market='+item+'&file_format=csv&odds_format=decimal&key=e372499bd7c3b6da5defd535d951'
    x=pd.read_csv(euro_url)
    tournament=x['event_name'].unique()[0]
    
    
    first_cols=['event_name','market','dg_id','player_name']
    mid_cols=['datagolf_base_history_fit','datagolf_baseline']
    start_cols=first_cols+mid_cols
    last_cols=[col for col in x.columns if col not in start_cols]
    last_cols=sorted(last_cols)
    columns=start_cols + last_cols
    columns.append(columns.pop(columns.index('last_updated')))
    x=x[columns]
    
    
    implied_url='https://feeds.datagolf.com/betting-tools/outrights?tour=pga&market='+item+'&file_format=csv&odds_format=percent&key=e372499bd7c3b6da5defd535d951'
    y=pd.read_csv(implied_url)
    
    y=y[['dg_id','datagolf_base_history_fit','datagolf_baseline']].add_suffix('_%')

    
    x=pd.merge(x,y,how='left',left_on='dg_id',right_on='dg_id_%').drop(columns=['dg_id_%'])
    
    start_cols=['event_name','market','dg_id','player_name','datagolf_base_history_fit','datagolf_baseline','datagolf_base_history_fit_%','datagolf_baseline_%']
    last_cols=[col for col in x.columns if col not in start_cols]
    last_cols=sorted(last_cols)
    columns=start_cols + last_cols
    columns.append(columns.pop(columns.index('last_updated')))
    x=x[columns]
    

    
    rankings_url='https://feeds.datagolf.com/preds/get-dg-rankings?file_format=csv&key=e372499bd7c3b6da5defd535d951'
    rankings=pd.read_csv(rankings_url)
    rankings=rankings[['dg_id','player_name','datagolf_rank','owgr_rank','dg_skill_estimate']]
    
    

    bets=pd.merge(x,rankings[['dg_id','datagolf_rank','owgr_rank']],how='left',on='dg_id')
    
    start_cols=['event_name','market','dg_id','player_name','datagolf_rank','owgr_rank','datagolf_base_history_fit','datagolf_baseline','datagolf_base_history_fit_%','datagolf_baseline_%']
    last_cols=[col for col in x.columns if col not in start_cols]
    last_cols=sorted(last_cols)
    columns=start_cols + last_cols
    columns.append(columns.pop(columns.index('last_updated')))
    bets=bets[columns]
    
    
    bets.to_csv('data/all_bets_outright/raw/'+tournament+'_'+today+'_'+item+'.csv',index=False)


    differences=bets.drop(columns=['last_updated','event_name','datagolf_rank','owgr_rank','datagolf_base_history_fit','datagolf_baseline'])
    start_cols=['market','dg_id','player_name','datagolf_base_history_fit_%','datagolf_baseline_%']
    last_cols=[col for col in differences.columns if col not in start_cols]
    last_cols=sorted(last_cols)
    columns=start_cols + last_cols
    differences=differences[columns]
    
    for col in last_cols:
        differences[col+'_exp_value']=differences['datagolf_base_history_fit_%']*differences[col]
        differences=differences.drop(columns=col)

   
    for col in last_cols:
        good_bets=differences[differences[col+'_exp_value']>=1]
        good_bets_list.append(good_bets)
        
        
    differences.to_csv('data/all_bets_outright/cleaned/'+tournament+'_'+today+'_'+item+'_differences.csv',index=False)


    print(item+' Done!')
    

all_good_bets=pd.concat(good_bets_list).drop_duplicates()
all_good_bets.to_csv('data/good_bets/'+tournament+today+'.csv',index=False)