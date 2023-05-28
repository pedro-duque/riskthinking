import pandas as pd

def kpi_calculate(df_feature_engineering):
    grouped_df = df_feature_engineering.groupby('Symbol')
    df_feature_engineering['vol_moving_avg'] = grouped_df['Volume'].rolling(window=30).mean().reset_index(0, drop=True)
    df_feature_engineering['adj_close_rolling_med'] = grouped_df['Volume'].rolling(window=30).median().reset_index(0, drop=True)

    return df_feature_engineering

def force_table_schema(df_feature_engineering):
    df_feature_engineering['Date'] = pd.to_datetime(df_feature_engineering['Date'])
    df_feature_engineering.sort_values('Date', inplace=True)

    return df_feature_engineering

def mutate_feature_engineering(df_feature_engineering):

    df_feature_engineering = force_table_schema(df_feature_engineering)
    df_feature_engineering = kpi_calculate(df_feature_engineering)

    return df_feature_engineering
