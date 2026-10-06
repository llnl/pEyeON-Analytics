"""Single home for metadata-type discovery, naming, and mapping.

Consolidates logic previously duplicated across ObservationHierarchy,
search_forms, EyeOnSummary, and Schema_Blame: which silver `metadata_*`
tables exist, which types dbt curates, friendly display names, and the
union-SQL builders that stitch the per-type tables together.
"""

import streamlit as st

import utils.db as db
from utils.sqlutil import sql_literal

METADATA_LABELS = {
    "metadata_binwalk_file": "Binwalk scan",
    "metadata_container_file": "Container",
    "metadata_device_tree_file": "Device tree",
    "metadata_elf_file": "ELF binary",
    "metadata_error": "Error",
    "metadata_generic_file": "Generic file",
    "metadata_java_file": "Java",
    "metadata_js_file": "JavaScript",
    "metadata_mach_o_file": "Mach-O",
    "metadata_native_lib_file": "Native library",
    "metadata_ole_file": "OLE document",
    "metadata_opkg_file": "OpenWrt package metadata",
    "metadata_pe_file": "PE binary",
    "metadata_symlink_file": "Symlink",
    "metadata_text_file": "Text/config/script",
    "metadata_uimage_file": "U-Boot image",
    "metadata_unknown": "Unknown",
    "metadata_web_asset": "Web asset",
}

# Curated/known types when dbt hasn't materialized gold_staging yet.
_FALLBACK_TYPE_KEYS = [
    "binwalk",
    "coff",
    "container",
    "device_tree",
    "elf",
    "error",
    "generic",
    "java",
    "js",
    "mach_o",
    "native_lib",
    "ole",
    "opkg",
    "pe",
    "symlink",
    "text",
    "uimage",
]


@st.cache_data(show_spinner=False)
def _silver_tables() -> list[str]:
    rows = (
        db.get_conn()
        .execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = 'silver'
              -- DuckDB LIKE treats '_' as a single-character wildcard, so avoid
              -- patterns like '%__%' which would match almost anything.
              and left(table_name, 9) = 'metadata_'
              and instr(table_name, '__') = 0
            order by table_name
            """
        )
        .fetchall()
    )
    return [str(row[0]) for row in rows]


@st.cache_data(show_spinner=False)
def _silver_table_columns() -> dict[str, set[str]]:
    rows = (
        db.get_conn()
        .execute(
            """
            select table_name, column_name
            from information_schema.columns
            where table_schema = 'silver'
              and left(table_name, 9) = 'metadata_'
            """
        )
        .fetchall()
    )
    columns: dict[str, set[str]] = {}
    for table_name, column_name in rows:
        columns.setdefault(str(table_name), set()).add(str(column_name))
    return columns


@st.cache_data(show_spinner=False)
def _curated_type_keys() -> list[str]:
    try:
        rows = (
            db.get_conn()
            .execute(
                """
                select table_name
                from information_schema.tables
                where table_schema = 'gold_staging'
                  and left(table_name, 13) = 'stg_metadata_'
                order by table_name
                """
            )
            .fetchall()
        )
    except Exception:
        return _FALLBACK_TYPE_KEYS

    keys: set[str] = set()
    for (table_name,) in rows:
        name = str(table_name)
        if not name.startswith("stg_metadata_"):
            continue
        key = name.removeprefix("stg_metadata_")
        if key.endswith("_file"):
            key = key.removesuffix("_file")
        if key:
            keys.add(key)

    # Historical naming mismatch: model drops `_file`, silver table keeps it.
    if "native_lib" not in keys and any(
        str(t[0]) == "stg_metadata_native_lib" for t in rows
    ):
        keys.add("native_lib")

    return sorted(keys)


class MetadataCatalog:
    """Discovery + naming for silver metadata tables and curated dbt types.

    Discovery results are cached with st.cache_data; call
    st.cache_data.clear() after loads that may introduce new types.
    """

    def silver_tables(self) -> list[str]:
        """Top-level silver.metadata_* tables (one per metadata type)."""
        return _silver_tables()

    def curated_type_keys(self) -> list[str]:
        """Curated/known type keys from dbt gold_staging models."""
        return _curated_type_keys()

    def silver_table_for(self, type_key: str) -> str | None:
        """Map a type key (dropdown value) to its silver base table name."""
        if type_key == "error":
            return "metadata_error"
        if type_key == "unknown":
            return "metadata_unknown"
        if type_key == "native_lib":
            return "metadata_native_lib_file"
        # Most types follow the `metadata_<type>_file` convention.
        return f"metadata_{type_key}_file"

    def short_name(self, table_name: str) -> str:
        """`metadata_pe_file` -> `pe`."""
        return table_name.removeprefix("metadata_").removesuffix("_file")

    def label(self, table_name: str) -> str:
        """Friendly display name for a metadata table."""
        return METADATA_LABELS.get(
            table_name,
            self.short_name(table_name).replace("_", " ").title(),
        )

    def loaded_type_names(self) -> list[str]:
        """Short names of metadata types present in gold.all_metadata.

        Live query (not cached); returns ["_None_"] when nothing is loaded.
        """
        tables = (
            db.get_conn()
            .execute(
                "select list_sort(list(distinct _metadata_table_name)) from gold.all_metadata"
            )
            .fetchone()[0]
        )
        if tables is None:
            return ["_None_"]
        return [self.short_name(s) for s in tables]

    def uuid_union_sql(self) -> str | None:
        """UNION ALL of uuids across all silver metadata tables.

        Returns None when no metadata tables exist yet.
        """
        tables = self.silver_tables()
        if not tables:
            return None
        return "\nunion all\n".join(
            [f"select uuid from silver.{t}" for t in tables]
        )

    def detail_union_sql(self) -> str:
        """UNION ALL of (uuid, metadata_table, metadata_type, extension,
        mime_type) across all silver metadata tables, tolerating tables that
        lack the optional columns."""
        columns_by_table = _silver_table_columns()
        selects = []
        for table_name in self.silver_tables():
            columns = columns_by_table.get(table_name, set())
            extension_expr = (
                "cast(extension as varchar)" if "extension" in columns else "NULL"
            )
            mime_expr = (
                "cast(mime_type as varchar)" if "mime_type" in columns else "NULL"
            )
            selects.append(
                f"""
                select
                  uuid,
                  '{table_name}' as metadata_table,
                  {sql_literal(self.label(table_name))} as metadata_type,
                  {extension_expr} as extension,
                  {mime_expr} as mime_type
                from silver.{table_name}
                """
            )
        if not selects:
            return (
                "select NULL as uuid, NULL as metadata_table, NULL as metadata_type, "
                "NULL as extension, NULL as mime_type where false"
            )
        return "\nunion all\n".join(selects)
