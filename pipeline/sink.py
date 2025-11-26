# simple sink module for writting to storage


# df: pyspark dataframe to write, path: output path, fmt: format (json in this case), mode: write mode (overwrite in this case)
def write_df(df, path, fmt="json", mode="overwrite"):
    if not path or not isinstance(path, str):
        raise ValueError(f"Path must be a non-empty string, got: {path}")

    df.write.format(fmt).mode(mode).save(path)