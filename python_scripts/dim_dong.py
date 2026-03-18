# https://github.com/vuski/admdongkor

import geopandas as gpd
from shapely.ops import orient
from datetime import datetime
from google.cloud import bigquery

def fix_orientation(geom):
    if geom is None:
        return None

    return orient(geom, sign = 1.0)

dim_dong = gpd.read_file('HangJeongDong_ver20260201.geojson')

dim_dong.drop(['sgg', 'sido'], axis = 1, inplace = True)

dim_dong.columns = ['DONG_NM_DETAIL', 'DONG_ID10', 'SIDO_NM', 'SGG_NM', 'DONG_ID', 'GEOMETRY']
if sum(dim_dong['GEOMETRY'].isna()) != 0:
    raise ValueError('GEOMETRY 컬럼이 비어있음')

dim_dong = dim_dong.set_geometry('GEOMETRY')
dim_dong['GEOMETRY'] = dim_dong.make_valid()
dim_dong['GEOMETRY'] = dim_dong['GEOMETRY'].apply(fix_orientation)

dim_dong['GEOMETRY'] = dim_dong['GEOMETRY'].apply(lambda x: x.wkt)
dim_dong['DONG_NM'] = dim_dong['DONG_NM_DETAIL'].apply(lambda x: x.split()[2])
dim_dong.drop('DONG_NM_DETAIL', axis = 1, inplace = True)

dim_dong['DONG_ID10'] = dim_dong['DONG_ID10'].astype('int')
dim_dong['DONG_ID'] = dim_dong['DONG_ID'].astype('int')

dim_dong['UPDATED_AT'] = datetime.today().date()

client = bigquery.Client()
table = 'data-engineering-478006.spark_dataset.dim_dong'
config = bigquery.LoadJobConfig(
    schema = [
        bigquery.SchemaField("GEOMETRY", "GEOGRAPHY"),
        bigquery.SchemaField("UPDATED_AT", "DATE")
    ]
)

job = client.load_table_from_dataframe(
    dataframe = dim_dong,
    destination = table,
    job_config = config
)

job.result()
