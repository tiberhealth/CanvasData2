# Canvas Data 2 Tool for Tiber Health Innovations and Partners
The Canvas Data 2 tool is a CLI tool that downloads the Canvas warehouse data from Instructure and creates the CSV files, SQL Create scripts, and SQL Data Load scripts for MySQL. The Canvas Data 2 tool by Tiber Health schools, campuses, and partner universities to pull their Canvas instance data and place it into the central warehouse. 

The tool utilized the Instructure-Dap client to pull the data from the Instructure. Due to performance and unique warehouse needs, we decided not to use the DAP data loader and wrote this tool to generate the CSV files and scripts to create the staging tables and load the data. 

This tool was written and tested using Python 3.11. 

The tool is currently set up only to handle snapshots, and future development will also include incremental data pulls.

# Usage
```python
Usage: main.py [OPTIONS]
```
| switch   [--]                             | Type                           | Description                                                                                       |            Default             |
|:------------------------------------------|:-------------------------------|:--------------------------------------------------------------------------------------------------|:------------------------------:|
| concurrent-limit                          | INTEGER                        | Max concurrent download threads                                                                   |               10               |
| max-download-attempts                     | INTEGER                        | Number of time to attempt to download a table if an error occurs before aborting orskipping table |               3                |
| semaphore-timeout-seconds                 | INTEGER                        | Number of seconds to wait for a semaphore lock before aborting or trying again                    |              120               |
| max-lock-attempts                         | INTEGER                        | Number of attempts allowed for grabbing a semaphore lock before throwing error                    |               3                |
| csv-field-size-limit                      | INTEGER                        | Maximum size of a CSV file                                                                        |           Max Value            |
| sleep-between-attempts-seconds            | INTEGER                        | Number of seconds to pause when a thread receives and error before attempting again               |               1                |
| thread-pause                              | INTEGER                        | Number of seconds to pause between download thread starts.                                        |              0.25              |
| log-level                                 | TEXT                           | Logging detail level.<br />Values: DEBUG, DETAIL,WARNING, ERROR, LOG_SYSTEM                       |             DETAIL             |
| schema-only<br />no-schema-only            | bool                           | Flag to only include the SQL schema scripts. No data download                                     | False<br />[ no-schema-only ]  |
| no-schema<br />no-no-schema                | bool                           | Flag to not generate the SQL Schema scripts                                                       |  False<br />[ no-no-schema ]   |
| include-sql-load<br />no-include-sql-load  | bool                           | Flag as to include the SQL Load statements in the SQL scripts                                     | True<br />[ include-sql-load ] |
| import-warnings<br />no-import-warnings    | bool                           | Flag to indicate if warnings and erros should be displayed after a table load                        | False<br>[ no-import-warnings] |
| workspace-root                            | TEXT                           | Root location for generated files                                                                 |          ./workspace           |
| csv-workspace                             | TEXT                           | Location for the table CSV files. Overrides the default location under --workspace_root           |              None              |
| sql-workspace                             | TEXT                           | Location for the table SQL and load script. Overrides the default location under --workspace_root |              None              |
| raw-workspace                             | TEXT                           | Location for the RAW data files. Overrides the default location under --workspace_root            |              None              |
| env-locale                                | TEXT                           | The locale setting for output strings and numbers                                                 |          en_US.UTF-8           |
| excluded-tables                           | TEXT                           | List of tables to exclude from the CD2 download. Semi-colon or comma separated                    |              None              |
| tables                                    | TEXT                           | Tables to download from CD2. Semi-colon orcomma seperated                                         |              None              |
| dap-yaml                                  | TEXT                           | Location of the YAML with the Canvas instance credentials                                         |          ./canvas.ynl          |
| settings-yaml                             | TEXT                           | Location of settings YAML File. YAML file override all switch options and defaults                |         ./defaults.yml         |
| install-completion                        | bash zsh fish powershell pwsh  | Install completion for the specified shell.                                                       |                                |
| show-completion                           | bash zsh fish powershell  pwsh | Show completion for the specified shell, to copy it or customize the installation.                |                                |
| help                                      |                                | Show this message and exit.                                                                       |                                | |

