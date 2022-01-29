import psycopg2
import pandas.io.sql as sqlio
import pygsheets
import pandas as pd
import exchange

def days_between(d1, d2):
    return abs((d2 - d1).days)

### import listing data
sheet_name = 'listing_winners'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheet_name)
worksheet = sheet[0]
listing_data = worksheet.get_as_df()

# clear listing data
indexes = listing_data[listing_data['Winner'] == '' ].index
listing_data.drop(indexes , inplace=True)
import re
listing_data['token_name'] = listing_data['Winner'].apply(lambda x: re.split('[(]|[)]', x)[1])
listing_data=listing_data.reset_index(drop=True)

### set BD connection
conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")
data = None
traders_data = None
for index, token in enumerate(listing_data['token_name']):

    token_id_query = """
    select id
    from view_asset_manager_currency
    where tag = '{}'
    LIMIT 1;
    """.format(token)
    token_id = sqlio.read_sql_query(token_id_query, conn)
    
    if (token_id.empty):
        print(token)
        continue
    else:
        token_id = token_id['id'][0]
    
    traders_query = """
    select id,
           trade_date
    from
    (select taker_trader as id,
            __create_date  as trade_date
    from view_market_aggregator_trade
    where currency = '{}'
    and taker_trader != maker_trader
    union
    select maker_trader as id,
            __create_date  as trade_date
    from view_market_aggregator_trade
    where currency = '{}'
    and taker_trader != maker_trader) as traders
    group by trade_date, id
    order by trade_date;
    """.format(token_id, token_id)
    token_traders = sqlio.read_sql_query(traders_query, conn)
    
    # calculate the quantity of individual ids per each week    
    if (token_traders.empty):
        print(token)
        continue
    
    start_date = token_traders['trade_date'][0]
    token_traders['relative_weeks'] = token_traders['trade_date'].apply(lambda x: 'Week ' + str(int(days_between(start_date, x)/7+1)))
    week = 1
    traders = pd.DataFrame(columns = ['Week', 'Traders']) 
    while True: 
        token_traders_week = token_traders[token_traders['relative_weeks'] == 'Week ' + str(week)]
        if (token_traders_week.empty):
            break
        token_traders_week.reset_index(drop=True, inplace=True)
        token_traders_week.drop_duplicates(subset='id', keep='first', inplace=True)
        values_to_add = {'Week': 'Week ' + str(week), 'Traders': len(token_traders_week)}
        row_to_add = pd.Series(values_to_add, name='x')
        traders = traders.append(row_to_add)
        week += 1
    
    volumes_query = """
    select trade.__create_date  as date,
           sum(trade.cost) as volume,
           quote_info.tag as quote_tag
    from view_market_aggregator_trade trade
    join view_asset_manager_currency quote_info
        on trade.quote = quote_info.id
    where trade.currency = '{}'
    and trade.taker_trader != trade.maker_trader
    group by date, quote_tag
    order by date;
    """.format(token_id, token_id)
    
    # get volumes' info and convert volumes to USDT
    token_volumes = sqlio.read_sql_query(volumes_query, conn)
    token_volumes = exchange.convert_to_USDT(token_volumes, columns=['volume'])
    token_volumes.drop(columns=['quote_tag'], inplace=True)
    
    fees_query = """
    select sum(trader_fee) as fee,
           trade_date as date,
           fee_cur_tag as quote_tag
    from
    (select trade.maker_fee as trader_fee,
            trade.__create_date as trade_date,
            currency.tag as fee_cur_tag
    from view_market_aggregator_trade trade
    join view_asset_manager_currency currency
        on trade.maker_fee_currency = currency.id
    where trade.currency = '{}'
    and trade.taker_trader != trade.maker_trader
    union all 
    select trade.taker_fee as trader_fee,
           trade.__create_date as trade_date,
           currency.tag as fee_cur_tag
    from view_market_aggregator_trade trade
    join view_asset_manager_currency currency
    on trade.taker_fee_currency = currency.id
    where trade.currency = '{}'
    and trade.taker_trader != trade.maker_trader) AS trade_table
    group by date, quote_tag
    order by date;
    """.format(token_id, token_id)
    
    # get fees' info and convert fees to USDT
    token_fees = sqlio.read_sql_query(fees_query, conn)
    token_fees = exchange.convert_to_USDT(token_fees, columns=['fee'])
    token_fees.drop(columns=['quote_tag'], inplace=True)
    
    # merge data
    token_data = token_volumes.merge(token_fees, how='left', on='date')
    token_data.set_index('date', inplace=True)
    
    # print empty tokens
    if (token_data.empty):
        print(token)
        continue
    
    # add additional info
    token_data.reset_index(level=0, inplace=True)
    token_data['token_name'] = token
    token_data['token_country'] = listing_data['project country'][index]
    
    traders['token_name'] = token
    traders['token_country'] = listing_data['project country'][index]
    traders['birth_date'] = start_date
   
    token_data['relative_days'] = token_data['date'].apply(lambda x: days_between(start_date, x))
    token_data['birth_date'] = start_date
    token_data['relative_weeks'] = token_data['date'].apply(lambda x: 'Week ' + str(int(days_between(start_date, x)/7+1)))
    token_data.set_index('date', inplace=True)
    
    try:
        data = data.append(token_data)
        traders_data = traders_data.append(traders)
    except:
        data = token_data
        traders_data = traders
        
import pygsheets

# set connection
data = data.rename(columns={'relative_weeks':'Weeks', 'relative_days':'Days', 'trades':'Traders', 'volume':'Volume', 'trader_fee':'Fees'})
sheet_name = 'listed_tokens_data'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheet_name)

#select the first sheet
worksheet = sheet[0]
worksheet.clear()
data.reset_index(level=0, inplace=True)
worksheet.set_dataframe(data, (1,1), fit=True)
            

sheet_name = 'Ind_Traders_per_weeks'
gc = pygsheets.authorize(service_file='funneldata-3e2cf01dc135.json')
sheet = gc.open(sheet_name)

#select the first sheet
worksheet = sheet[0]
worksheet.clear()
data.reset_index(level=0, inplace=True)
worksheet.set_dataframe(traders_data, (1,1), fit=True)
            









