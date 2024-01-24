import boto3
import argparse
import yaml, os, sys
import awswrangler as wr
from jinja2 import Environment, FileSystemLoader, pass_context


JINJA_MACROS_PATH = os.path.dirname(os.path.abspath(__file__)).rstrip('/') + '/jinja_macros/'
SCRIPT_DESC = 'Upload config files inside the config folder to S3'

@pass_context
def call_macro_by_name(context, macro_name, *args, **kwargs):
    return context.vars[macro_name](*args, **kwargs)


def render_template(file_path = None, template_str = None):
    """
    Render a given file/string, replacing Jinja stuff.

    Args:
        file_path (string): An optional file path to load the input string
        template_str (string): An optional string to provide instead of the file path

    Returns:
        The rendered string
    """

    if file_path is not None:
        template_str = open(file_path).read()

    jinja2_environment = Environment(loader=FileSystemLoader(JINJA_MACROS_PATH))
    jinja2_environment.filters['macro'] = call_macro_by_name
    return jinja2_environment.from_string(template_str)


def generate_cleaned_sql(view_name, conf, source_database, dest_database):
    """
    Generate the SQL cleaned view using the Jinja2 template, using a config YAML file.

    Args:
        view_name (string): The given view name
        conf (dict): Config file (from the YAML) with columns names and options

    Returns:
        sql: the SQL query generated
    """

    sql = "{% import 'macros.sql' as macros %} \n"
    sql += f"CREATE OR REPLACE VIEW {dest_database}.{view_name} AS \n"

    i = 0
    for col_name in conf:
        col_type = conf[col_name]['type']

        if 'skip' in conf[col_name] and conf[col_name]['skip']:
            continue

        # Each column can optionally be renamed, can have a default value, can be required
        as_col_name = conf[col_name]['rename'] if 'rename' in conf[col_name] else col_name

        col_required = ", conversion_required=true " if 'conversion_required' in conf[col_name] and conf[col_name]['conversion_required'] else ""

        if col_type == 'string':
            col_default = f", default_value='{conf[col_name]['default']}'" if 'default' in conf[col_name] else ""
        else:
            col_default = f", default_value={conf[col_name]['default']}" if 'default' in conf[col_name] else ""
        col_parameters = f", parameters={conf[col_name]['parameters']}" if 'parameters' in conf[col_name] else ""

        if 'clean_actions' not in conf[col_name]:
            conf[col_name]['clean_actions'] = []

        conf[col_name]['clean_actions'].append('convert_nan_na_to_null')
        col_actions = f", actions={conf[col_name]['clean_actions']}"

        macro_params = f' "{col_name}" {col_required} {col_default} {col_actions} {col_parameters}'
        sql += " SELECT \n   " if i == 0 else "\n   , "
        sql += "{{ macros.convert_to_" + col_type + "( " + macro_params + ") }} AS " + as_col_name
        i += 1

    sql += f"\nFROM {source_database}.t_{view_name}_v1" # TODO fix v

    return sql


def list_diff(li1, li2):
    """ Get difference of two lists """
    return list(set(li1) - set(li2)) + list(set(li2) - set(li1))


def get_script_args():
    """
    Load script arguments

    Returns:
        dictionary of arguments
    """

    parser = argparse.ArgumentParser(description=SCRIPT_DESC)
    parser.add_argument('-p', '--profile',
        help='Your AWS Profile name',
        required=True
    )
    parser.add_argument('--stage',
        help='The AWS environment (loads values from conf-env/{env}.yml) [default: %(default)s]',
        choices=['dev', 'prod'],
        default='dev'
    )
    parser.add_argument('--view',
        help='The view name to load'
    )
    parser.add_argument('--db',
        help='The db name to filter'
    )
    parser.add_argument('-v', '--verbose',
        action='store_true',
        help='Optionally print more info'
    )
    parser.add_argument('--show_null_columns',
        action='store_true',
        help='Print nan columns for each table'
    )

    return parser.parse_args()


args = get_script_args()

if args.verbose:
    print("> Using Verbose Mode")

# Read Environment file
stage_conf_path = f"conf-env/{args.stage}.yml"
with open(stage_conf_path, "r") as stream:
    try:
        stage_conf = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

if not args.show_null_columns:
    print(f"\n{SCRIPT_DESC}\n---")
    print(f"Region: {stage_conf['aws_region_name']}")
    print("---\n\n")


boto3.setup_default_session(
    profile_name=args.profile,
    region_name=stage_conf['aws_region_name']
)


# Loop through config files and execute the query
for root, dirs, files in sorted(os.walk(f'./config/{args.stage}')):

    for file in files:
        if file.endswith('.yml'):

            database = root.split('/')[3]
            source_database = database
            dest_database = database
            view_name = file.replace('.yml', '')

            sql_descriptor = {
                'view_name': view_name
            }

            file_path = os.path.join(root, file)

            if args.db is not None and database != args.db:
                continue

            if args.view is not None and view_name != args.view:
                continue

            if args.show_null_columns:
                print(f"\n> {view_name}")

            with open(file_path, "r") as conf_stream:
                try:
                    conf = yaml.safe_load(conf_stream)
                except yaml.YAMLError as exc:
                    print(exc)

            if not len(conf):
                continue

            sql = generate_cleaned_sql(view_name, conf, source_database, dest_database)

            if not args.show_null_columns:
                print(f"\n\n-------------------------------\n> Executing query {file_path}")
            template = render_template(template_str=sql)

            query = template.render(**sql_descriptor)

            if args.verbose:
                print(query)

            # https://aws-data-wrangler.readthedocs.io/en/stable/stubs/awswrangler.athena.start_query_execution.html
            query_exec_id = wr.athena.start_query_execution(sql=query, database=database)
            if not args.show_null_columns:
                print(f"Waiting for query execution {query_exec_id}")

            res = wr.athena.wait_query(query_execution_id=query_exec_id)
            if not args.show_null_columns:
                print(f" > Status: {res['Status']['State']}")

            if res['Status']['State'] != 'SUCCEEDED':
                print(f"\n{query}\n")
            else:
                # Check if columns casted are the same as one given as input
                # (useful to check if all the various coalesce ends with a null)
                df_check = wr.catalog.table(database=dest_database, table=view_name)

                errs = 0

                # Adjusting conf dict for type check with rename column values
                iteration_conf = conf.copy()
                for key in iteration_conf.keys():
                    if 'rename' in conf[key]:
                        rename_value = conf[key]['rename']
                        type_value = conf[key]['type']
                        conf[rename_value] = {'type': type_value}
                        del conf[key]

                for i in range(0, len(df_check)):
                    c_name = df_check.iloc[i]['Column Name']
                    c_type = df_check.iloc[i]['Type']

                    if conf[c_name]['type'] == 'datetime' and c_type in ('date', 'datetime'):
                        continue # OK (for now)

                    if conf[c_name]['type'] != c_type:
                        print(f" >> Error: Column {c_name} needs to be {conf[c_name]['type']} but {c_type} as been set.")
                        errs += 1

                if not errs:
                    if not args.show_null_columns:
                        print(" > Type check ok")
                else:
                    print(f" > WARNING: Type check failed with {errs} error(s)")

                # Also check if the query runs fine
                try:
                    df = wr.athena.read_sql_query(
                        f"SELECT * FROM {view_name} LIMIT 1000"
                        , database=dest_database
                        , ctas_approach=True
                    )

                    if not args.show_null_columns:
                        print(" > Query check ok")

                    if args.verbose:
                        print("\n HEAD")
                        print(df.head())

                    if args.verbose or args.show_null_columns:

                        na_cols = df.columns[df.isna().all()].tolist()
                        uniq_cols_with_null = df.columns[df.nunique() <= 1].tolist()
                        uniq_cols = list_diff(uniq_cols_with_null, na_cols)

                        na_str_cols = []
                        nan_str_cols = []
                        for c in uniq_cols:
                            if df[c].astype(str).str.contains('<NA>').any():
                                na_str_cols.append(c)
                            elif df[c].astype(str).str.contains('nan').any():
                                nan_str_cols.append(c)

                        print("\n None columns: ", na_cols)
                        print("\n '<NA>' columns: ", na_str_cols)
                        print("\n 'nan' columns: ", nan_str_cols)

                except Exception as e:
                    print(f" > WARNING: Query failed with error [[ {e} ]]")

            print(f"\n\n")