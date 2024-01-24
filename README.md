# Data Lake Cleaning Process

This repository includes the code for the cleaning process of the data already acquired by previous ingestion phases.

The process is triggered manually using the `load_views.py` script, which takes these parameters:

- `-p|--profile` (optional) to specify the AWS profile (if you have a default or an export of the ACCESS/SECRET keys this can be left empty)
- `--stage` (optional, default to "dev") one of "dev" or "prod". This is used to load the environment file
- `--view` (optional) The view name to be load instead of process all of them
- `--db` (optional) The db name to filter
- `-v|--verbose` (optional) print more info
- `--show_null_columns` (optional) Print nan columns for each table

Example processing only for specific view:

```shell
~$ python load_views.py --profile {your-aws-profile-name} --stage {stage} --db my_database --view my_table
```

## How it works?

This project is Python based and works with the Jinja2 templating to replace macros inside dynamic generated SQL code, to prevent code repetition and more cleaning: each SQL query is created dynamically based on the content of the corresponding YAML file and represents a view in a database.
The main script loads these views into AWS Athena (with the CREATE OR REPLACE statement).

Jinja2 must be installed first:

```shell
~$ pip install Jinja2
```

The script also performs some additional operations:

- Checks if remote column types are the same of the YAML-defined ones
- Execute a SELECT ALL on the table to check if runs fine. It is also possible to show first 5 results using the "verbose" option (see script parameters)
- Optionally (with the show_null_columns parameter of the main script) show the list of columns which contains only nan/na/null values

## Jinja2 Macro Parameters

Each Jinja2 macro converts a column to a certain type (int, bigint, ...) and accepts the following parameters:

- `field` (required): The field name to be typed.
- `default_value` (optional): The value empty/null cells needs to be set to. It defaults to `NULL`.
- `conversion_required` (optional): If set to `true`, every non-empty cell **must** be castable to the desired type. If set to `false`, cells that are not castable to the appropriate type will be converted to `default_value`. It defaults to `False`.
- `actions` (optional): List of cleaning actions.

This parameters are sent throught the config file (see the `Config files` section for more examples).


## Requirements

It is important to respect the following structure of folders/files:

- `jinja-macros`: a folder which contains the main `macros.sql` files on which each of the functions/*.sql macros arer included. These files will be automatically loaded and replaced inside the cleaned views if needed. The are also some pre-defined "actions" (like "keep only numbers" or "remove na" from columns) which can be easily extended inside the `jinja-macros/functions/actions.sql` file.

- `config`: a folder which contains multiple folders, one for each database. Each of these subfolder contains the YAML config file, one for each view to be created.

- Add any new macro to `macros.sql` under `/jinja-macros`

## Config files

As said, each of the config files have the definitions to generate dynamically each view.
The base syntax is:

```yml
column_name:
    type: column_type
```

with column type which can currently be one of:

- bigint
- int
- double
- date
- timestamp
- boolean
- string

Each of these can have a default value, i.e.:

```yml
my_column_1:
  type: int
  default: 0
```

Each of these can be required, i.e.:

```yml
my_column_2:
  type: string
  conversion_required: true
```

Each of these can have additional cleaning actions to be applied on the columns before the type conversion, i.e.:

```yml
my_column_3:
  type: string
  clean_actions:
  - keep_only_numbers
  - remove_na
```
