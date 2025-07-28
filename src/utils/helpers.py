import json, pathlib

def pretty(obj): print(json.dumps(obj, indent=2, default=str))
def proj_root() -> pathlib.Path: return pathlib.Path(__file__).parents[2]
