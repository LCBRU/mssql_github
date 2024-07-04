import schedule
import shutil
import time
import logging
from logging.handlers import SMTPHandler
import os
import subprocess
from dotenv import load_dotenv
import json
import argparse
from datetime import date

class ApplicationError(Exception):
    def __init__(self, message):
        self.message = message


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

    ddl_dir = os.path.join(repository_dir, 'ddl')

    if os.path.exists(ddl_dir):
        shutil.rmtree(ddl_dir)

    for d in databases:
        db_backup_dir = os.path.join(ddl_dir, d)

        logging.info(f'Scripting {d} to {db_backup_dir}')

        completed_process = subprocess.run(
            args=[
                'mssql-scripter',
                '--server', server,
                '--user', user,
                '--password', password,
                '-d', d,
                '--file-path', db_backup_dir,
                '--file-per-object',
                '--exclude-headers',
                '--exclude-use-database',
            ],
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )

        if len(completed_process.stdout) > 0:
            raise ApplicationError(f'Error scripting "{d}":\n\n{completed_process.stdout}')


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

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(logging_format)

email_handler = SMTPHandler(
    mailhost=os.environ['SMTP_SERVER'],
    fromaddr=os.environ['FROM_EMAIL'],
    toaddrs=[os.environ['ERROR_RECIPIENT']],
    subject='MSSQL_GITHUB ERROR',
)
email_handler.setLevel(logging.ERROR)
email_handler.setFormatter(logging_format)

logger.addHandler(stream_handler)
logger.addHandler(email_handler)

args = get_parameters()

try:
    if args.run:
        run()
    else:
        schedule_scripting()
except ApplicationError as e:
    logging.error(e.message)
