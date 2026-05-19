<img width="1280" height="640" alt="Delta Rollback Orchestrator" src="https://github.com/user-attachments/assets/30e4062a-71d2-49e4-8312-a312716c7a5f" />



# **The Mass Emergency Button for Restoring Tables to a Stable Version**

<br>

This is a process a team hopes to never use, as it acts as an "emergency button." **Imagine** a bad deployment or a malfunctioning routine reaches a layer causing inconsistencies or corrupted data in many schemas and tables, this is the fastest way to revert everything to the previous stable state without needing to reprocess pipelines or take reports offline while deciding what to do.

The premise is simple in concept but critical in execution: use Delta Lake's Time Travel to perform a mass rollback of distributed tables across multiple **schemas** to the last stable version of a specific day (or, using the default behavior, the latest update from the previous day). In a single run, the notebook can handle dozens of tables scattered across dozens of **schemas**, identifying the exact version each one should revert to.

The project was built on Databricks but, with small adjustments, it can run on any Spark engine that supports the Delta Lake format regardless of the underlying cloud provider. The only Databricks-specific pieces are `dbutils.widgets` for parameter input and the Unity Catalog `three-part namespace`; both can be swapped out with minimal effort.

---

## **1. 🎛️ Parameters (Widgets) and Operation Modes**

<br>


The entire notebook is controlled by 5 widgets at the top of the page. Before any execution, it's worth understanding each one, especially `schema_name`s and `restore_date`, which change their behavior depending on the input.

**🔸 1. `catalog` - Target Catalog**
Defines in which catalog of `Databricks Unity Catalog`  the tables will be searched in. The catalog name is passed down to `SHOW SCHEMAS`, which will list all schemas in this catalog, excluding those in a `"BLOCK_LIST"`.

| Value | Behavior |
| :--- | :--- |
| `catalog_name` | Executes against the specified catalog and lists all the schemas that belong to it. |


<br>

**🔸 2. `schema_names` - Target Schemas**
Here lies one of the most useful parts of the script: the execution mode changes depending on what is passed as a parameter.

| Value | Behavior |
| :--- | :--- |
| `default` | Batch mode. Processes all schemas in the catalog, except those on the internal blocklist (more on this later). |
| A single specified schema | Restricts to a single schema. Useful in surgical scenarios where only one schema was affected. |
| Multiple specified schemas | Restricts to a comma-separated list of schemas. The order is preserved. |
| Non-existent schema in the list | The script lists all invalid schemas together before failing, always detailing everything to the user. |

<br>

**🔸 3. `table_names` - Target Tables**
Defines which tables will be restored within each processed schema. Extremelly usefull if you have many schemas that have the same table nomenclature.

| Value | Behavior |
| :--- | :--- |
| Using a single table | Restores only the specified table for the schema or schemas provided in the `schema_names` parameter above. |
| Using multiple tables | Restores multiple tables in the provided order. |
| If the user inputs nothing. | Fails the SAFE INPUT validation before any processing. All widgets are protected by a system I call `"SAFE INPUT"`, which prevents the user from typing forbidden characters or any input deemed unsuitable for the system. |

<br>

**🔸 4. `execution_mode` - Execution Mode**
A dropdown with only two options. It controls the only real difference between simulating and executing.

| Value | Behavior |
| :--- | :--- |
| `dry_run` (default) | Full simulation. Validates everything, fetches the history, and builds the SQL command, but does not trigger the RESTORE. It only prints the command that would be executed. |
| `execute` | Real execution. Triggers the `RESTORE TABLE ... TO VERSION AS OF <id>` for each eligible table. |

> ⚠️ **Operational best practice:** a `dry_run` is practically "free" in terms of time and carries zero risk. In production, always run in this mode first and review the final summary before switching to `execute`.

<br>

**🔸 5. `restore_date` - Target RESTORE Date**
Defines which day the script will use as an "anchor point" to fetch the last commit. This widget provides the flexibility to revert to any date within the Delta retention window.

| Value | Behavior |
| :--- | :--- |
| `default` (default) | Uses yesterday as the target date. The classic "undo what was done today" behavior; restores to the latest version of the previous day. |
| `2026-05-10` (YYYY-MM-DD format) | Uses the specified date as the target. Useful when the issue went unnoticed for a few days. |
| ⚠️ Invalid format (`10/05/2026`, `2026-05-32`, etc.) | Fails with a message explaining the expected format. |
| ⚠️ Future date (`2027-01-01`) | Explicitly rejected; there are no commits in the future. |

<br>

---

## **2. ⚙️ The Strategy: Design Pillars**

---

<br>

The architecture was designed around three central concerns. It is worth understanding them before touching the code, as they explain several seemingly the decisions throughout the notebook.

**Resilience via Isolated Loop**
Each operation (schema × table pair) is handled within its own `try/except` block. If a schema's table fails—whether because older files were vacuumed via `VACUUM` or due to a specific permissions issue—the script logs the failure for that specific target and proceeds normally with the rest.

**Defense-in-Depth for Inputs**
Before any SQL is even built, the `SAFE INPUT` module validates all inputs: empty fields, blocked characters (`'`, `"`, `[`, `]`, `{`, `}`, `;`, `.`), date formats, future dates, and non-existent schemas in the catalog. The goal is to fail fast and clearly, preventing problematic strings from getting near crucial commands.

**Deterministic Versioning, Not Timestamps**
The script could have used `RESTORE TABLE ... TO TIMESTAMP AS OF '...'`, but the safer route was chosen: it queries `DESCRIBE HISTORY`, identifies the exact ID of the latest version on the target date, and uses `TO VERSION AS OF <id>`. Versions are immutable and exact; timestamps can become ambiguous when commits occur very close to the day's boundary.

<br>

---

## **3. 🔧 How it Works: The Notebook's Logic**

---

<br>

The notebook is organized into 6 cells, each with a very clear responsibility.

**Step 1: Capturing Widgets and Constants**
The first cell reads the 5 widgets, applies `.strip().lower()` to inputs that need normalization, and defines two constants used throughout the code:
* **`FORBIDDEN_CHARS`** - A set of blocked characters for any user input. Acts as a defense against dynamic SQL injection.
* **`ERROR_HINT`** - A standardized message that appears whenever the user tries to use a blocked character. Keeps the error tone consistent.

**Step 2: Defining Auxiliary Functions**
The second cell centralizes reusable logic into four functions:
* **`is_delta_table(full_table_name)`** - Checks via `DESCRIBE DETAIL` if the table is in Delta format. Necessary because `RESTORE` only works on Delta tables.
* **`get_last_version_of_date(full_table_name, target_date)`** - Runs `DESCRIBE HISTORY`, filtering commits between `00:00:00` and `23:59:59` on the target date, sorts descending by version, and returns the most recent `version`. Returns `None` if there are no commits on that date.
* **`validate_widget_input(value, field_name, empty_hint)`** - Validates that inputs (single string or list) are not empty and do not contain blocked characters.
* **`resolve_restore_date(value)`** - Converts the widget string into a `date` object. Handles `default`, the `YYYY-MM-DD` format, invalid dates (e.g., `2026-02-30`), and future dates.

**Step 3: SAFE INPUT — Critical Validations**
Here, the validations are actively applied. Order matters:
1. Performs a `split(",")` to transform `table_names` and `schema_names` into lists, removing spaces and empty items.
2. Calls `validate_widget_input` for each widget, blocking empty values and forbidden characters before any SQL is built.
3. Resolves `restore_date` into a `date` object via `resolve_restore_date`.
4. If any validation fails, the notebook dies before running `SHOW SCHEMAS`. Failing fast here saves cluster time and keeps the error close to its root cause.

**Step 4: Schema Identification**
This does three things, in this order:
1. Prints the header containing the catalog, mode, target tables, and the RESTORE target date. This print is critical; it's where the operator verifies they are about to run the right thing.
2. Runs `SHOW SCHEMAS IN <catalog>`, applies the internal blocklist (which ignores, for example, `bronze`, `silver`, `gold`, `information_schema`, `sys`, `system`, etc.), and resolves the final list of schemas to be processed.
3. If `schema_names` is a specific list, it validates that all schemas exist within the catalog. If any do not exist, it lists all invalid ones together in the error message, rather than making the operator discover them one execution at a time.

**Step 5: Execution Engine (Nested Loop)**
The heart of the process. For each combination (schema × table), the flow is:

Below, `failures` and `successes` are counters used to track how many operations were executed successfully or failed. It will also describe why the operation was ignored or failed (e.g., `ignored_...`).
```text
does the table exist? ──── no ──→ skip (skipped_not_found_count)
       │
      yes
       │
is it Delta format?   ──── no ──→ skip (skipped_not_delta_count)
       │
      yes
       │
is there a commit on target date? ── no ──→ skip (skipped_no_commit_count)
       │
      yes
       │
is the mode execute?  ──── no ──→ dry_run (prints command, success_count ++)
       │
      yes
       │
triggers RESTORE TABLE ... TO VERSION AS OF <id>
       │
       success?        ── yes → success_count ++
                       └─ no → failure_count ++ & adds to failed_tables 
