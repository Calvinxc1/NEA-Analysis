import numpy as np
import pandas as pd

def build_material_matrix(material_data, product_data, mat_eff_mapping, addit_matrix=None):
    material_names = material_data[['type_id', 'type_name']].drop_duplicates().set_index('type_id')['type_name'].sort_index()
    product_names = product_data[['type_id', 'type_name']].drop_duplicates().set_index('type_id')['type_name'].sort_index()

    bp_type_map = product_data.set_index('blueprint_id')['type_id']
    material_matrix = product_data[['blueprint_id', 'type_id']].merge(material_data, on='blueprint_id').rename(columns={
        'type_id_x': 'product_type_id',
        'type_id_y': 'material_type_id'
    })
    material_matrix = material_matrix.pivot(index='product_type_id', columns='material_type_id', values='quantity').fillna(0)
    multiplyer = 1 / material_matrix.index.map(product_data.set_index('type_id')[['quantity', 'probability']].product(axis=1)).values[:,np.newaxis]
    mat_eff = 1 - (bp_type_map.to_frame().join(mat_eff_mapping.rename('mat_eff').to_frame()).drop_duplicates().set_index('type_id')['mat_eff']/100)
    mat_eff = material_matrix.index.map(mat_eff).values[:,np.newaxis]
    if np.any(pd.isnull(mat_eff)):
        raise Exception("mat_eff_mapping doesn't cover all present blueprint_id's")

    material_matrix = np.ceil((material_matrix * mat_eff).round(2)) * multiplyer

    if addit_matrix is not None:
        full_index = sorted(list(set([*material_matrix.index, *addit_matrix.index])))
        full_columns = sorted(list(set([*material_matrix.columns, *addit_matrix.columns])))
        reindex_mat_matrix = material_matrix.reindex(index=full_index, columns=full_columns).fillna(0)
        reindex_addit_matrix = addit_matrix.reindex(index=full_index, columns=full_columns).fillna(0)
        material_matrix = reindex_mat_matrix + reindex_addit_matrix
    else:
        material_matrix = material_matrix.loc[sorted(material_matrix.index), sorted(material_matrix.columns)]

    step_proc_mats_cols = material_matrix.columns[material_matrix.columns.isin(material_matrix.index)]
    while step_proc_mats_cols.size > 0:
        step_proc_mats = material_matrix.loc[:,step_proc_mats_cols] @ material_matrix.loc[step_proc_mats_cols,:]
        material_matrix = material_matrix.drop(columns=step_proc_mats_cols) + step_proc_mats.drop(columns=step_proc_mats_cols)
        step_proc_mats_cols = material_matrix.columns[material_matrix.columns.isin(material_matrix.index)]
        
    return material_matrix, material_names, product_names