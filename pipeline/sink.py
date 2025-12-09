# simple sink module for writting to storage

# df: pyspark dataframe to write
# path: output path
# fmt: format (json in this case)
# mode: write mode (overwrite in this case)
# ValueError if path is not valid
def write_df(df, path: str, fmt: str = "json", mode: str = "overwrite"):
    if not path or not isinstance(path, str):
        raise ValueError(f"Path must be a non-empty string, got: {path}")

    df.write.format(fmt).mode(mode).save(path)