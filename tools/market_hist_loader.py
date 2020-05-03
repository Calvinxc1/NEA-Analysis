import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nea_schema.esi.mkt import MarketHist

def load_market_hist(type_ids, region_ids, start_date, end_date, sql_params):
    engine = create_engine('{engine}://{user}:{passwd}@{host}/{db}'.format(**sql_params))
    Session = sessionmaker(bind=engine)
    conn = Session()
    
    query = conn.query(
        MarketHist.record_date,
        MarketHist.region_id,
        MarketHist.type_id,
        MarketHist.volume,
        MarketHist.average,
    )\
        .filter(MarketHist.type_id.in_(type_ids))\
        .filter(MarketHist.region_id.in_(region_ids))\
        .filter(MarketHist.record_date >= start_date)\
        .filter(MarketHist.record_date <= end_date)
    raw_data = pd.read_sql(query.statement, query.session.bind)
    
    
    market_data = {}
    for region_id, data in raw_data.groupby('region_id'):
        market_data[region_id] = {}
        for values in ['volume', 'average']:
            pivot_cols = {
                'index': 'record_date',
                'columns': 'type_id',
                'values': values,
            }
            market_data[region_id][values] = data[pivot_cols.values()].pivot(**pivot_cols)\
                .fillna(method='ffill').fillna(method='bfill')
    
    return market_data