import pandas as pd
import glob

events='https://feeds.datagolf.com/get-event-list?tour=pga&file_format=csv&key=e372499bd7c3b6da5defd535d951'

event_ids=pd.read_csv(events).sort_values(by=['calendar_year','event_id'])
event_ids=event_ids[event_ids['archived_preds']!='no']
event_ids=event_ids[(event_ids['outrights']!='no')|(event_ids['matchups']!='no')]
event_ids=event_ids[['event_name','event_id','calendar_year']].reset_index(drop=True)

book_list=['bet365', 'betcris', 'betmgm', 'betway', 'bovada', 'corale', 'draftkings', 'fanduel', 'pinnacle', 'skybet', 'sportsbook', 'unibet', 'williamhill']
market_list=['win','top_5','top_10','top_20','make_cut','mc']

path='data/cleaned_rankings/dg_ranks_'

for i in event_ids.index:
    event_outright_list=[]
    event_matchups_list=[]
    pretourney_list=[]
    year=str(event_ids.loc[i]['calendar_year'])
    event_id=str(event_ids.loc[i]['event_id'])
    event_name=str(event_ids.loc[i]['event_name'])
    
    for file_name in glob.glob(path+'*event='+event_id+'_year='+year+'.csv'):
        rankings=pd.read_csv(file_name)
    
    for market in market_list:
        pre_tournament_prediction='https://feeds.datagolf.com/preds/pre-tournament-archive?event_id='+event_id+'&year='+year+'&odds_format=percent&file_format=csv&key=e372499bd7c3b6da5defd535d951'
        pre_tourney=pd.read_csv(pre_tournament_prediction)
        pre_tourney=pd.merge(pre_tourney,rankings[['player_name','dg_rank','owgr_rank']],how='left',on=['player_name'])
        if market=='mc':
            market_1='make_cut'
        else:
            market_1=market
        pre_tourney['market']=market
        pre_tourney=pre_tourney[['market','event_completed','event_id','event_name','model','dg_id','player_name','dg_rank','owgr_rank','fin_text',market_1]]
        
        pretourney_list.append(pre_tourney)
        for book in book_list:
            try:
                old_odds='https://feeds.datagolf.com/historical-odds/outrights?event_id='+event_id+'&tour=pga&year='+year+'&market='+market+'&book='+book+'&odds_format=decimal&file_format=csv&key=e372499bd7c3b6da5defd535d951'
                old_odds_df=pd.read_csv(old_odds)
                old_odds_df=old_odds_df[['market','year','event_id','event_name','dg_id','player_name','bet_outcome_numeric','outcome','open_odds','close_odds','book']]
                old_odds_df=pd.merge(old_odds_df,pre_tourney,how='left',on=['event_id','event_name','dg_id','player_name','market'])
                old_odds_df['exp_return']=old_odds_df['close_odds']*old_odds_df[market_1]
                event_outright_list.append(old_odds_df)
            except:
                print('***************'+book +' '+event_id+' '+year+' did not work for OUTRIGHTS*****************')
                
            try:
                old_matchups='https://feeds.datagolf.com/historical-odds/matchups?event_id='+event_id+'&tour=pga&year='+year+'&market='+market+'&book='+book+'&odds_format=decimal&file_format=csv&key=e372499bd7c3b6da5defd535d951'
                old_matchups_df=pd.read_csv(old_matchups)
                old_matchups_df=pd.merge(old_matchups_df,rankings[['player_name','dg_rank','owgr_rank']].add_prefix('p1_'),how='left',on='p1_player_name')
                old_matchups_df=pd.merge(old_matchups_df,rankings[['player_name','dg_rank','owgr_rank']].add_prefix('p2_'),how='left',on='p2_player_name')
                old_matchups_df=pd.merge(old_matchups_df,rankings[['player_name','dg_rank','owgr_rank']].add_prefix('p3_'),how='left',on='p3_player_name')
                old_matchups_df=old_matchups_df[['bet_type','year','event_id','event_name','p1_dg_id','p1_player_name','p1_dg_rank','p1_owgr_rank','p1_outcome','p1_open','p1_close',
                                                 'p2_dg_id','p2_player_name','p2_dg_rank','p2_owgr_rank','p2_outcome','p2_open','p2_close','p3_dg_id','p3_player_name','p3_dg_rank','p3_owgr_rank','p3_outcome','p3_open','p3_close','tie_rule','book']]
                
                event_matchups_list.append(old_matchups_df)
    
            
            except:
                print('***************'+book +' '+event_id+' '+year+' did not work for MATCHUPS*****************')
                
    pretourney=pd.concat(pretourney_list)
    outrights=pd.concat(event_outright_list)
    matchups=pd.concat(event_matchups_list)
    
    
    pretourney.to_csv('data/pretournament/'+event_name+'_'+year+'.csv',index=False)        
    outrights.to_csv('data/posttournament/outright/'+event_name+'_'+year+'.csv',index=False)     
    matchups.to_csv('data/posttournament/matchups/'+event_name+'_'+year+'.csv',index=False)     


