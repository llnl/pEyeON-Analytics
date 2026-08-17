---
title: "Feature Design: Cleanup Streamlit App"
type: concept
confidence: high
grounded_by:
  - ../pEyeON-Analytics/EyeOnData.py
  - ../pEyeON-Analytics/utils/utils.py
  - ../pEyeON-Analytics/pages/pages.py
policy: agent-editable
last_validated: 2026-08-17
repo_scope: pEyeON-Analytics
implementation_area: streamlit
format_domain: none
audience: mixed
status: draft
source_paths: wiki/work/cleanup-streamlit-app/design.md
tags: [feature-work, streamlit, ui, cleanup, design]
---

# Feature Design: Cleanup Streamlit App

Living design document; phases are settled with the Architect incrementally.

## Summary

Overhaul `EyeOnData.py` in Architect-directed phases. Phase 1 (settled
2026-08-17): migrate to Streamlit's modern `st.navigation`/`st.Page`
navigation and remove dead code — nothing else.

## Phase 1 (settled by Architect 2026-08-17)

**In scope:**
- Migrate to `st.navigation`/`st.Page` (streamlit 1.58 installed; pyproject
  pins `streamlit>=1.56.0`). Replaces the hand-rolled `pages/pages.py`
  registry, per-page `sidebar_config(app_pages())` calls, and per-page
  `st.set_page_config` boilerplate.
- Remove dead code: `run_eyeon()` (never called; reads a session key nothing
  sets), the vestigial `BasePageLayout`/`LandingPage` class layer, and the
  now-superseded `app_base_config`/`sidebar_config`/`pages/pages.py`/
  `pages/_base_page.py`.

**Explicitly out of scope for Phase 1 (Architect direction):**
- Fragile coupling fixes (`root_table_selector` session key, hardwired
  `raw_obs`/`bronze.raw_json` hacks) — leave as is.
- Schema chooser issues (`schema_list.index("silver")` crash risk) — leave
  as is.
- Authentication — leave as is (none).

## Proposed Approach (Phase 1)

### Entrypoint (`EyeOnData.py`)

The entrypoint owns app-wide config and navigation; it runs on every rerun:

1. Single `st.set_page_config(...)` with the app title, logo icon, and
   `layout="wide"` (every existing page already used wide; the init form
   inherits it — acceptable change).
2. Sidebar logo + app title (common elements).
3. If `db.exists()`: `st.navigation([...six st.Page entries...])` with
   EyeOn Summary as `default=True`, then `sidebar_db_chooser()` so the
   schema/root-table sidebar renders on every page exactly as before, then
   `pg.run()`.
4. Else: `st.navigation([st.Page(init page)])` — the init-database form
   becomes the only page until a DB exists (replaces the old
   `st.switch_page` + "Virtual Main Page" fallback markdown).

### Pages

Each page file keeps its filename and content functions but sheds:
`st.set_page_config` (owned by entrypoint), `sidebar_config(app_pages())`
(owned by entrypoint/navigation), the `LandingPage(BasePageLayout)` wrapper,
and registry imports. Page files end with a module-level `main()` call
guarded by `if __name__ in ("__main__", "__page__")` — `"__page__"` is the
name Streamlit assigns when running a file via `st.Page`; `"__main__"`
preserves direct `streamlit run pages/x.py` use.

Per-page browser-tab titles (previously set via per-page
`st.set_page_config(page_title=...)`) are dropped; the tab shows the app
title and the sidebar nav shows per-page titles from `st.Page(title=...)`.
Accepted simplification.

### utils/utils.py

- Delete `run_eyeon` (and its then-unused `subprocess` import).
- Delete `app_base_config` and `sidebar_config` (superseded by entrypoint).
- Keep everything else unchanged: `init_app_form`, `batch_selector`, batch
  parsing, `load_me_some_data`, `run_dbt`, `load_data`, `_db_settings`,
  `sidebar_db_chooser`, `list_dirs`, `list_all_batches`.

### Deletions

- `pages/pages.py` (registry → `st.Page` list in entrypoint)
- `pages/_base_page.py` (vestigial ABC)

## Data Model Or Schema Changes

None.

## Interfaces And User Experience

- Sidebar: navigation menu is rendered by `st.navigation` at the top of the
  sidebar; logo/title and the DB chooser follow it (previously logo/title
  were above the menu). Minor visual reordering, accepted.
- Behavior parity otherwise: same six pages, same default landing page, same
  init-form flow, same per-page content.

## Edge Cases

- No-DB startup: nav collapses to the single init page; after a successful
  load, `st.rerun()` re-executes the entrypoint, `db.exists()` flips, and
  full navigation appears.
- `BrowseDltData` depends on `st.session_state.root_table_selector` from
  `_db_settings`; the entrypoint renders `sidebar_db_chooser()` before
  `pg.run()` on every rerun, so the key exists exactly as before.
- Direct page runs (`streamlit run pages/certs.py`) still execute content
  via the `__main__` guard but without the entrypoint's sidebar/config —
  same as legacy behavior minus the old per-page boilerplate.

## Error Handling

Unchanged; page-level guards (missing models, empty tables) stay as they are.

## Risks

- `__page__` module-name behavior is version-specific; guarded by including
  both names. Verified against installed streamlit 1.58.
- Dropping per-page `set_page_config` changes tab titles; flagged above.

## Alternatives Considered

- **Callable-based `st.Page(func)` for all pages** — rejected for Phase 1:
  converting six script files into imported functions is a bigger diff with
  no behavioral gain; file-based pages keep review small.
- **Keep `pages/pages.py` registry feeding `st.Page` entries** — rejected:
  the registry's only consumer was the hand-rolled sidebar; keeping it adds
  indirection `st.navigation` already provides.

## Open Questions

Later-phase questions remain in [[wiki/work/cleanup-streamlit-app/brief]]
(UIX direction, auth, `common/`/`extras/` dispositions, `utils/utils.py`
split).
