import folium as fm
import pandas as pd
import numpy as np
from folium.plugins import HeatMap


def create_shipping_heatmap(
    db,
    conn,
    *args,
    **kwargs,
) -> fm.Map:
    df = db.get_shipping(conn)
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    m = fm.Map(
        location=(df.latitude.mean(), df.longitude.mean()),
        zoom_start=6,
        tiles='cartodbpositron'
    )
    HeatMap(
        [(i['latitude'], i['longitude']) for _, i in df.iterrows()]
    ).add_to(m)
    return m


def id_nan_to_none(
    df: pd.DataFrame,
    *args,
    **kwargs,
) -> pd.DataFrame:
    ''' Replace np.nan with None in any column with "id" in its name

    Useful since Postgres does not allow NaN values in the Bigint datatype, but
    None values is fine.
    '''
    df[list(df.columns[df.columns.str.contains('id')])].replace({np.nan: None})
    return df
