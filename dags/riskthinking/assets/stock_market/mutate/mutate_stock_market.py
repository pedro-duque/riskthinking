import polars as pl

def wipeout_file_extention(pl_df_stock_market):

    pl_df_stock_market = pl_df_stock_market.with_columns(
        pl.col("File_name").str.replace(".csv", "")
    )

    return pl_df_stock_market

def merge_symbols_valid_meta(pl_df_stock_market, pl_df_symbols_valid_meta):
    pl_df_symbols_valid_meta = pl_df_symbols_valid_meta.select(
        [
            pl.col("Symbol"),
            pl.col("Security Name")
        ]
    )

    pl_df_stock_market = pl_df_stock_market.join(pl_df_symbols_valid_meta, left_on="File_name", right_on="Symbol", how="left")
    
    return pl_df_stock_market

def force_table_schema(pl_df_stock_market):

    pl_df_stock_market = pl_df_stock_market.select(
        [   
            pl.col("File_name").cast(pl.Utf8).alias("Symbol"),
            pl.col("Security Name").cast(pl.Utf8),
            pl.col("Date").cast(pl.Utf8),
            pl.col("Open").cast(pl.Float64),
            pl.col("High").cast(pl.Float64),
            pl.col("Low").cast(pl.Float64),
            pl.col("Close").cast(pl.Float64),
            pl.col("Adj Close").cast(pl.Float64),
            pl.col("Volume").cast(pl.Float64)
        ]
    )

    return pl_df_stock_market

def mutate_stock_market(pl_df_stock_market, pl_df_symbols_valid_meta):
    
    pl_df_stock_market = wipeout_file_extention(pl_df_stock_market)
    pl_df_stock_market = merge_symbols_valid_meta(pl_df_stock_market, pl_df_symbols_valid_meta)
    pl_df_stock_market = force_table_schema(pl_df_stock_market)
    
    return pl_df_stock_market