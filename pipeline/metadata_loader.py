import json

# get json metadata from file
def load_metadata(path):
    with open(path, "r") as f:
        return json.load(f)
