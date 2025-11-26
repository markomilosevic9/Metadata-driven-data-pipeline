import json
import pytest
from pathlib import Path

# 1st pre-pipeline test, checks for config integrity
# checks for metadata structure and internal consistency
# chain of functions to achieve it

# get metadata from designated location
def load_metadata():
    metadata_path = Path("config/metadata_motor.json")
    with open(metadata_path, 'r') as f:
        return json.load(f)

# check if metadata file is valid json file
def test_metadata_valid_json():
    try:
        metadata = load_metadata()
        assert metadata is not None
        assert isinstance(metadata, dict)
    except json.JSONDecodeError as e:
        pytest.fail(f"Metadata file is not valid JSON file: {e}")

# check if metadata file has required top-level structure / dataflows
def test_metadata_has_required_structure():
    metadata = load_metadata()
    
    assert "dataflows" in metadata, "No dataflows key in metadata"
    assert isinstance(metadata["dataflows"], list), "dataflows must be a list"
    assert len(metadata["dataflows"]) > 0, "At least 1 dataflow must be defined"


# check if each dataflow has required sections: sources, transformations, sinks
def test_dataflow_has_required_sections():
    metadata = load_metadata()
    
    for i, flow in enumerate(metadata["dataflows"]):
        assert "name" in flow, f"Dataflow {i} missing name"
        assert "sources" in flow, f"Dataflow '{flow.get('name', i)}' missing sources"
        assert "transformations" in flow, f"Dataflow '{flow.get('name', i)}' missing transformations"
        assert "sinks" in flow, f"Dataflow '{flow.get('name', i)}' missing sinks"
        
        assert isinstance(flow["sources"], list), f"sources must be a list in dataflow '{flow['name']}'"
        assert isinstance(flow["transformations"], list), f"transformations must be a list"
        assert isinstance(flow["sinks"], list), f"sinks must be a list"


# check if all sources have required fields: name, path, format
def test_sources_have_required_fields():
    metadata = load_metadata()
    
    for flow in metadata["dataflows"]:
        for source in flow["sources"]:
            assert "name" in source, "Source missing name field"
            assert "path" in source, f"Source '{source.get('name')}' missing path"
            assert "format" in source, f"Source '{source.get('name')}' missing format"
            
            # path validation
            path = source["path"]
            assert path.startswith("s3a://"), f"Source path must start with s3a://: {path}"
            assert len(path) > 7, f"Source path suspiciously short: {path}"

# verify transformations have valid structure: name, type, params
def test_transformations_have_valid_structure():
    metadata = load_metadata()
    
    allowed_types = ["validate_fields", "add_fields"]
    
    for flow in metadata["dataflows"]:
        for transform in flow["transformations"]:
            assert "name" in transform, "Transformation missing name"
            assert "type" in transform, f"Transformation '{transform.get('name')}' missing type"
            assert "params" in transform, f"Transformation '{transform.get('name')}' missing params"
            
            # type validation
            transform_type = transform["type"]
            assert transform_type in allowed_types, f"Unknown transformation type '{transform_type}', {allowed_types} are allowed"


# check validations use supported rule names
def test_validation_rules_are_valid():
    metadata = load_metadata()
    
    allowed_rules = ["notNull", "notEmpty", "regex", "minValue"]
    
    for flow in metadata["dataflows"]:
        for transform in flow["transformations"]:
            if transform["type"] == "validate_fields":
                validations = transform["params"].get("validations", [])
                
                for validation in validations:
                    field = validation.get("field", "UNKNOWN")
                    rules = validation.get("rules", [])
                    
                    for rule in rules:
                        if isinstance(rule, str):
                            assert rule in allowed_rules, f"Unknown validation rule '{rule}' for field '{field}', {allowed_rules} are allowed"
                        
                        elif isinstance(rule, dict):
                            rule_name = rule.get("name", "UNKNOWN")
                            assert rule_name in allowed_rules, f"Unknown validation rule '{rule_name}' for field '{field}', {allowed_rules} are allowed"
                            
                            assert "params" in rule, "Rule '{rule_name}' for field '{field}' missing 'params'"


# check sinks inputs 
def test_sinks_reference_valid_inputs():
    metadata = load_metadata()
    
    for flow in metadata["dataflows"]:
        available_views = set()
        
        for source in flow["sources"]:
            available_views.add(source["name"])
        
        for transform in flow["transformations"]:
            if transform["type"] == "validate_fields":
                available_views.add("validation_ok")
                available_views.add("validation_ko")
            elif transform["type"] == "add_fields":
                available_views.add(transform["name"])
        
        # check sinks
        for sink in flow["sinks"]:
            sink_name = sink.get("name", "UNKNOWN")
            sink_input = sink.get("input")
            
            assert sink_input, f"Sink '{sink_name}' missing 'input' field"
            assert sink_input in available_views, \
                f"Sink '{sink_name}' references unknown input '{sink_input}'. " \
                f"Available: {sorted(available_views)}"

# check if sink paths are formatted properly
def test_sinks_have_valid_paths():
    metadata = load_metadata()
    
    for flow in metadata["dataflows"]:
        for sink in flow["sinks"]:
            sink_name = sink.get("name", "UNKNOWN")
            
            # check if path exists
            assert "path" in sink, f"Sink '{sink_name}' missing 'path' field"
            
            path = sink["path"]
            assert path, f"Sink '{sink_name}' has empty path"
            assert path.startswith("s3a://"), f"Sink path must start with 's3a://': {path}"
            
            # check format
            assert "format" in sink, f"Sink '{sink_name}' missing 'format' field"

