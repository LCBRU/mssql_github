import schedule
import shutil
import time
import logging
import os
import subprocess as sp
from dotenv import load_dotenv
import json
import argparse
from datetime import date


def run():
    databases_env = os.getenv('DATABASES', "[]")
    logging.info(f'Databases: "{databases_env}"')
    databases = json.loads(databases_env)
    server = os.environ['MSSQL_SERVER']
    user = os.environ['MSSQL_USER']
    password = os.environ['MSSQL_PASSWORD']
    repository_parent_dir = os.environ['REPOSITORY_PARENT_DIR']
    repository_name = os.environ['REPOSITORY_NAME']

    repository_dir = os.path.join(repository_parent_dir, repository_name)

    if os.path.exists(repository_dir):
        shutil.rmtree(repository_dir)

    sp.run(
        [
            'git',
            'clone',
            f'https://github.com/LCBRU/{repository_name}.git',
        ],
        cwd=repository_parent_dir,
    )

    ddl_dir = os.path.join(repository_dir, 'ddl')

    if os.path.exists(ddl_dir):
        shutil.rmtree(ddl_dir)

    for d in databases:
        db_backup_dir = os.path.join(ddl_dir, d)

        logging.info(f'Scripting {d} to {db_backup_dir}')

        sp.run([
            'mssql-scripter',
            '--server', server,
            '--user', user,
            '--password', password,
            '-d', d,
            '--file-path', db_backup_dir,
            '--file-per-object',
            '--exclude-headers',
            '--exclude-use-database',
        ])

    logging.info(f'Committing and pushing repository at {repository_dir}')
        
    sp.run(
        [
            'git',
            'add',
            '-A',
        ],
        cwd=repository_dir,
    )

    sp.run(
        [
            'git',
            'commit',
            '-m',
            f'{date.today():%d/%m/%Y}',
        ],
        cwd=repository_dir,
    )

    sp.run(
        [
            'git',
            'push',
        ],
        cwd=repository_dir,
    )

    if os.path.exists(repository_dir):
        shutil.rmtree(repository_dir)


def get_parameters():
    parser = argparse.ArgumentParser(description='Script MS SQL databases and check them into Github')
    parser.add_argument(
        "-r",
        "--run",
        help="Run scripting",
        action="store_true",
    )

    args = parser.parse_args()

    return args


def schedule_scripting():
    schedule.every().day.at("05:00").do(run)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info('Schedule stopped')


load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

args = get_parameters()

if args.run:
    run()
else:
    schedule_scripting()
