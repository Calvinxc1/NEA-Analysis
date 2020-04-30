import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from nea_schema.esi.corp import CorpBlueprint
from nea_schema.sde.inv import Type
from nea_schema.sde.bp import Blueprint, Activity, Material, Product

def pull_bp_data(sql_params):
    engine = create_engine('{engine}://{user}:{passwd}@{host}/{db}'.format(**sql_params))
    Session = sessionmaker(bind=engine)
    conn = Session()
    
    avail_bps = parse_avail_bps(conn)
    bp_data, activity_data, material_data, product_data = parse_bp_data(conn, avail_bps.index)
    return avail_bps, bp_data, activity_data, material_data, product_data

def parse_avail_bps(conn):
    query = conn.query(
        CorpBlueprint.item_id,
        CorpBlueprint.type_id,
        Type.type_name,
        CorpBlueprint.material_efficiency.label('mat_eff'),
        CorpBlueprint.time_efficiency.label('time_eff'),
    ).filter(CorpBlueprint.quantity == -1).join(Type)
    data = pd.read_sql(query.statement, query.session.bind, index_col='item_id')
    
    mat_mask = data.groupby('type_id')['mat_eff'].max().reset_index()
    masked_data = data.merge(mat_mask, on=['type_id', 'mat_eff'], how='inner')

    time_mask = masked_data.groupby('type_id')['time_eff'].max().reset_index()
    masked_data = masked_data.merge(time_mask, on=['type_id', 'time_eff'], how='inner')

    avail_bps = masked_data.drop_duplicates().set_index('type_id')
    
    return avail_bps

def parse_bp_data(conn, bp_types):
    query = conn.query(
        Blueprint.blueprint_id,
        Blueprint.type_id,
        Blueprint.max_production_limit,
    ).filter(Blueprint.type_id.in_(bp_types))
    bp_data = pd.read_sql(query.statement, query.session.bind, index_col='blueprint_id')
    
    query = conn.query(Activity.blueprint_id, Activity.time)\
        .filter(Activity.blueprint_id.in_(bp_data.index))\
        .filter(Activity.activity_type == 'manufacturing')
    activity_data = pd.read_sql(query.statement, query.session.bind, index_col='blueprint_id')
    
    query = conn.query(Material.blueprint_id, Material.type_id, Type.type_name, Material.quantity)\
        .filter(Material.blueprint_id.in_(bp_data.index))\
        .filter(Material.activity_type == 'manufacturing')\
        .join(Type)
    material_data = pd.read_sql(query.statement, query.session.bind)
    
    query = conn.query(Product.blueprint_id, Product.type_id, Type.type_name, Product.quantity, Product.probability)\
        .filter(Product.blueprint_id.in_(bp_data.index))\
        .filter(Product.activity_type == 'manufacturing')\
        .join(Type)
    product_data = pd.read_sql(query.statement, query.session.bind)
    
    return bp_data, activity_data, material_data, product_data