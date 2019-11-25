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
    databases = json.loads(os.getenv('DATABASES', "[]"))
    server = os.environ['MSSQL_SERVER']
    user = os.environ['MSSQL_USER']
    password = os.environ['MSSQL_PASSWORD']
    repository_dir = os.environ['REPOSITORY_DIR']
    backup_subdir = os.environ['BACKUP_SUBDIR']

    for d in databases:
        db_backup_dir = os.path.join(repository_dir, backup_subdir, d)
        print(f" Scripting {d} to {db_backup_dir}")
        
        shutil.rmtree(db_backup_dir)
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

        sp.run([
            'git',
            'add',
            '-A',
        ])

        sp.run([
            'git',
            'commit',
            '-m',
            f'"{date.today():%d/%m/%Y}"',
        ])

        sp.run([
            'git',
            'push',
        ])


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
    schedule.every(1).minutes.do(run)

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
