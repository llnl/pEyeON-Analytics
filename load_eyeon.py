import argparse
import base64
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import socket

import dlt
import duckdb

import utils.schema_blame as schema_blame
from utils.config import duckdb_path, resolve_dlt_path

# Validate UUIDs
UUID_RE = re.compile(r'"uuid"\s*:\s*"([^"]+)"')
logger = logging.getLogger(__name__)


def sanitize_ole_metadata(item):
    """Sanitize binary thumbnail data before loading"""

    if "ole" in item and item["ole"] is not None:
        ole = item["ole"]
        if "thumbnail" in ole and ole["thumbnail"] is not None:
            try:
                # Base64 encode binary data
                item["ole"]["thumbnail"] = base64.b64encode(
                    ole["thumbnail"].encode("latin-1")
                ).decode("ascii")
                logger.debug(
                    "Base64 encoded OLE thumbnail for %s",
                    item.get("uuid", "unknown"),
                )
            except Exception as e:
                # Log and null out problematic thumbnails
                logger.error(
                    "Error sanitizing thumbnail for %s: %s",
                    item.get("uuid", "unknown"),
                    e,
                )
                item["ole"]["thumbnail"] = None
    if "elfNote" in item and item["elfNote"] is not None:
        for elfNote in item["elfNote"]:
            if "descdata" in elfNote and elfNote["descdata"] is not None:
                try:
                    # Base64 encode binary data
                    elfNote["descdata"] = base64.b64encode(
                        elfNote["descdata"].encode("latin-1")
                    ).decode("ascii")
                    logger.debug(
                        "Base64 encoded ELF note descdata for %s",
                        item.get("uuid", "unknown"),
                    )
                except Exception as e:
                    # Log and null out problematic descdata
                    logger.error(
                        "Error sanitizing descdata for %s: %s",
                        item.get("uuid", "unknown"),
                        e,
                    )
                    elfNote["descdata"] = None

    return item


def drop_empty_lists(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            v2 = drop_empty_lists(v)
            # remove keys where the value is an empty list
            if isinstance(v2, list) and len(v2) == 0:
                logger.debug("Dropping empty list field: %s", k)
                continue
            out[k] = v2
        return out

    if isinstance(obj, list):
        return [drop_empty_lists(x) for x in obj]

    return obj


@dlt.source(name="eyeon_metadata")
def eyeon_source(utility_id, source, depth):
    # Create a single row identifying the batch. Use the 'load_id' which is also tracked thru the pipeline load info as _dlt_load_id.
    @dlt.resource(table_name="batch_info", write_disposition="append")
    def batch_resource():
        yield {
            "_dlt_load_id": dlt.current.load_package_state()["load_id"],
            "run_ts": datetime.now(),
            "utility_id": utility_id,
            "source": source,
            "depth": depth,
            "hostname": socket.gethostname(),
        }

    # The raw JSON data from the file. Note that valid JSON is loaded into the "JSON" field, which is also JSON type in duckdb. If the JSON is invalid it will be loaded into json__v_text as a string.
    # The primary purpose of this table is forensics. If this doesn't prove valuable, it could be elimated and we can just go back to the original file.
    @dlt.resource(
        name="raw_json",
        write_disposition="append",
        columns={"json": {"data_type": "json", "nullable": True}},
    )
    def raw_json_resource():
        for path in Path(source).glob("*.json"):
            with open(path, "r") as f:
                content = f.read()
            match = UUID_RE.search(content)
            yield {
                "uuid": match.group(1) if match else None,
                "json": content,
                "source_path": os.path.dirname(path),
                "source_file": os.path.basename(path),
            }

    # Resource for the main files table
    @dlt.resource(
        name="files_resource",
        table_name="raw_obs",
        write_disposition="merge",
        primary_key="uuid",
        max_table_nesting=depth,
    )
    def files_resource():
        for path in Path(source).glob("*.json"):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                    if data["uuid"] is None or len(data["uuid"]) != 36:
                        raise (
                            Exception(f"No UUID or invalid defined for file: {path}")
                        )
                    # Remove metadata which will prevent DLT from trying to unravel it and persist here. That gets done with custom code in `metadata_resource()`
                    data.pop("metadata", None)
                    # Add the source path and filename
                    data["source_path"] = os.path.dirname(path)
                    data["source_file"] = os.path.basename(path)
                yield drop_empty_lists(data)
            except Exception as e:
                # Yield error record instead of failing
                logging.error(f"JSON Error: {str(e)}\n\n{str(path)}")
                _json_errors.append(
                    {
                        "source_path": os.path.dirname(path),
                        "source_file": os.path.basename(path),
                        "_error": str(e),
                        "_parse_failed": True,
                        "_code_source": "file_resource",
                    }
                )

    # Resource for metadata: dispatches to per-type tables dynamically
    @dlt.resource(
        table_name=lambda item: item["_metadata_table_name"],
        write_disposition="append",  # or "merge" if you add a PK for metadata
        max_table_nesting=depth,
    )
    def metadata_resource():
        for path in Path(source).glob("*.json"):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                    uuid = data.get("uuid")

                    for md_type, raw_md_data in data.get("metadata", {}).items():
                        md_data = sanitize_ole_metadata(raw_md_data)

                        record = {
                            **md_data,  # unpack type-specific fields,
                            "uuid": uuid,  # FK back to files.uuid
                            "_metadata_table_name": f"metadata_{md_type.lower()}",
                        }
                        if md_type != "java_file":
                            yield record
                        else:
                            # For javaClasses, convert the dict key class name to a field
                            # If there are no classes, just remove the empty definition
                            if "javaClasses" in md_data and isinstance(
                                md_data["javaClasses"], dict
                            ):
                                # Convert the dict with class name keys into a list
                                # Pop the javaClasses list. This also removes it from md_data.
                                classes = md_data.pop("javaClasses")
                                class_list = []
                                for classname, class_data in classes.items():
                                    class_data["classname"] = (
                                        classname  # Add classname as a field
                                    )
                                    class_list.append(class_data)

                                md_data["javaClasses"] = class_list
                                md_data["_metadata_table_name"] = (
                                    f"metadata_{md_type.lower()}"
                                )
                                # Assign the key to the parent
                                md_data["uuid"] = uuid
                                #                                md_data["_dlt_parent_id"] =  parent_dlt_id # The parent key for dlt. UUID is the canonical key.
                                yield md_data

            except Exception as e:
                # Yield error record instead of failing
                _json_errors.append(
                    {
                        "source_path": os.path.dirname(path),
                        "source_file": os.path.basename(path),
                        "error": str(e),
                        "parse_failed": True,
                        "code_source": "metadata_resource",
                        "_metadata_table_name": "json_errors",
                    }
                )

    # Dedicated error table resource. Static name = static table = clean ownership.
    # Errors from metadata_resource still route here via _metadata_table_name,
    # which is fine — both resolve to the same table name "json_errors" and
    # DLT merges them without conflict because the table already exists.
    @dlt.resource(
        name="json_errors", table_name="json_errors", write_disposition="append"
    )
    def errors_resource():
        yield from _json_errors

    # Errors are collected into an array, which is processed as a resource.
    _json_errors = []

    return (
        batch_resource(),
        files_resource(),
        metadata_resource(),
        errors_resource(),
        raw_json_resource(),
    )


def print_schema_changes(load_info, load_name):
    # Access schema changes from the current run
    # Note: this uses the metadata that is written to a file by DLT.
    # A more comprehensive approach is implemented in `schema_blame.py`
    logger.info("Schema changes from the %s pipeline:", load_name)
    for package in load_info.load_packages:
        for table_name, table in package.schema_update.items():
            for column_name, column in table["columns"].items():
                logger.info(
                    "Table: %s, Column: %s, Type: %s",
                    table_name,
                    column_name,
                    column["data_type"],
                )


def parse_args():
    parser = argparse.ArgumentParser(
        prog="load_eyeon.py", description="Load EyeOn JSON into raw tables."
    )
    parser.add_argument(
        "--utility_id",
        required=True,
        help="Utility company ID, which is a short, unique string LLNL uses",
    )
    parser.add_argument("--source", required=True, help="Source path of JSON files")
    parser.add_argument(
        "--depth",
        required=False,
        default=4,
        help="Depth that DLT will attempt to parse for complex types",
    )
    parser.add_argument(
        "--log-level",
        required=False,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        type=str.upper,
        help="Set the log verbosity",
    )

    args = parser.parse_args()
    return vars(args)


def main(utility_id, source, depth=4, log_level="INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    DB_PATH = str(duckdb_path())
    conn = duckdb.connect(DB_PATH)  # native duckdb connection

    src = eyeon_source(utility_id, source, depth)  # has 4 resources

    pipeline = dlt.pipeline(
        pipeline_name="eyeon_metadata",
        destination=dlt.destinations.duckdb(conn),
        dataset_name="bronze",  # default schema (can override per run)
        dev_mode=False,
        export_schema_path=str(resolve_dlt_path("schemas")),
    )

    bronze = src.with_resources("raw_json")
    silver = src.with_resources(
        "batch_resource", "files_resource", "json_errors", "metadata_resource"
    )

    bronze_info = pipeline.run(
        bronze
    )  # loads raw_json into bronze schema, no parsing of JSON
    print_schema_changes(bronze_info, "bronze")
    silver_info = pipeline.run(
        silver, dataset_name="silver"
    )  # Parses JSON into several different tables into silver schema
    print_schema_changes(silver_info, "silver")

    # Process any schema changes and perist in the database
    schema_blame.materialize_schema_blame(conn)


if __name__ == "__main__":
    args = parse_args()
    main(**args)
