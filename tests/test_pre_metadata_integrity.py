import json
import pytest
from pathlib import Path
from pipeline.config_loader import load_config, get_metadata_path


# pre-pipeline metadata integrity tests
# validates that metadata file is well-formed and complete (internal consistency)
# does not validate business logic or pipeline implementation details
# focus is just a structural completeness, not logical correctness

# get metadata from designated location via config_loader, returns metadata dict
def _load_metadata():

    config = load_config()
    metadata_path = get_metadata_path(config)
    
    with open(metadata_path, 'r') as f:
        return json.load(f)


# basic structure test


# check if metadata file is valid json file
def test_metadata_valid_json():
    try:
        metadata = _load_metadata()
        assert metadata is not None, "Metadata loaded as None"
        assert isinstance(metadata, dict), f"Metadata must be a dictionary, got {type(metadata)}"
    except json.JSONDecodeError as e:
        pytest.fail(f"Metadata file is not valid JSON: {e}")

# check if metadata file has required top-level structure / dataflows key
def test_metadata_has_required_structure():
    metadata = _load_metadata()
    
    assert "dataflows" in metadata, "Metadata missing 'dataflows' key"
    assert isinstance(metadata["dataflows"], list), "dataflows must be a list"
    assert len(metadata["dataflows"]) > 0, "At least one dataflow must be defined"

# check if each dataflow has required sections: sources, transformations, sinks
def test_dataflow_has_required_sections():
    metadata = _load_metadata()
    
    for i, flow in enumerate(metadata["dataflows"]):
        flow_name = flow.get('name', f'dataflow_{i}')
        
        # check if required keys exist
        assert "name" in flow, f"Dataflow {i} missing 'name' field"
        assert "sources" in flow, f"Dataflow '{flow_name}' missing 'sources' section"
        assert "transformations" in flow, f"Dataflow '{flow_name}' missing 'transformations' section"
        assert "sinks" in flow, f"Dataflow '{flow_name}' missing 'sinks' section"
        
        # check types
        assert isinstance(flow["sources"], list), \
            f"sources must be a list in dataflow '{flow_name}'"
        assert isinstance(flow["transformations"], list), \
            f"transformations must be a list in dataflow '{flow_name}'"
        assert isinstance(flow["sinks"], list), \
            f"sinks must be a list in dataflow '{flow_name}'"


# incremental mode checks

# check if metadata has processing_mode field for incremental processing
def test_metadata_has_processing_mode():
    metadata = _load_metadata()
    
    assert "processing_mode" in metadata, \
        "Metadata missing 'processing_mode' field. Required for incremental processing."
    
    processing_mode = metadata["processing_mode"]
    assert processing_mode in ["incremental", "full"], \
        f"processing_mode must be 'incremental' or 'full', got: {processing_mode}"

# check if metadata has batch_config section for incremental mode
def test_metadata_has_batch_config():
    metadata = _load_metadata()
    
    processing_mode = metadata.get("processing_mode")
    
    if processing_mode == "incremental":
        assert "batch_config" in metadata, \
            "Metadata missing 'batch_config' section. Required when processing_mode is 'incremental'."
        
        batch_config = metadata["batch_config"]
        assert "input_pattern" in batch_config, \
            "batch_config missing 'input_pattern' field"
        assert "date_format" in batch_config, \
            "batch_config missing 'date_format' field"
        
        # check if input_pattern contains {date} placeholder
        input_pattern = batch_config["input_pattern"]
        assert "{date}" in input_pattern, \
            f"input_pattern must contain '{{date}}' placeholder, got: {input_pattern}"

# check if metadata has consolidation config
def test_metadata_has_consolidation_config():
    metadata = _load_metadata()
    
    processing_mode = metadata.get("processing_mode")
    
    if processing_mode == "incremental":
        assert "consolidation" in metadata, \
            "Metadata missing 'consolidation' section. Required for incremental processing."
        
        consolidation = metadata["consolidation"]
        assert "enabled" in consolidation, \
            "consolidation missing 'enabled' field"


# source completeness tests

# check if all sources have required fields: name, path, format and if paths are non-empty or incorrectly formatted
# does not validate schema definition, that is role of schema_enforcer module
def test_sources_have_required_fields():
    metadata = _load_metadata()
    
    for flow in metadata["dataflows"]:
        for source in flow["sources"]:
            # check if required fields exist
            assert "name" in source, "Source missing 'name' field"
            assert "path" in source, f"Source '{source.get('name')}' missing 'path' field"
            assert "format" in source, f"Source '{source.get('name')}' missing 'format' field"
            
            # check if path is non-empty and properly formatted
            path = source["path"]
            assert path, f"Source '{source.get('name')}' has empty path"
            assert path.startswith("s3a://"), \
                f"Source path must start with 's3a://', got: {path}"
            assert len(path) > 7, \
                f"Source path suspiciously short: {path}"


# transformations tests


# check if transformations have required fields: name, type, params
# does not validate transform types/params structure, that is role of runner/validator modules
def test_transformations_have_required_fields():
    metadata = _load_metadata()
    
    for flow in metadata["dataflows"]:
        for transform in flow["transformations"]:
            # check if required fields exist
            assert "name" in transform, "Transformation missing 'name' field"
            assert "type" in transform, \
                f"Transformation '{transform.get('name')}' missing 'type' field"
            assert "params" in transform, \
                f"Transformation '{transform.get('name')}' missing 'params' field"
            
            # check if fields are non-empty
            assert transform["name"], "Transformation has empty name"
            assert transform["type"], \
                f"Transformation '{transform['name']}' has empty type"
            assert isinstance(transform["params"], dict), \
                f"Transformation '{transform['name']}' params must be a dictionary"


# sinks tests 

# checks if sinks have required fields: name, input, path, format and if they are non-empty and correctly formatted
def test_sinks_have_required_fields():
    metadata = _load_metadata()
    
    for flow in metadata["dataflows"]:
        for sink in flow["sinks"]:
            sink_name = sink.get("name", "UNKNOWN")
            
            # check if required fields exist
            assert "name" in sink, "Sink missing 'name' field"
            assert "input" in sink, f"Sink '{sink_name}' missing 'input' field"
            assert "path" in sink, f"Sink '{sink_name}' missing 'path' field"
            assert "format" in sink, f"Sink '{sink_name}' missing 'format' field"
            
            # check if path is non-empty and properly formatted
            path = sink["path"]
            assert path, f"Sink '{sink_name}' has empty path"
            assert path.startswith("s3a://"), \
                f"Sink path must start with 's3a://', got: {path}"