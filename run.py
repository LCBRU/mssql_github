import schedule
import time
import logging
import os
from dotenv import load_dotenv
import json

def job():
    databases = json.loads(os.getenv('DATABASES', "[]"))

    for d in databases:
        print(d)

    # os.system('mssql-scripter --server UHLSQLBRICCSDB\\\\UHLBRICCSDB --user briccs_admin --password bR1cc5100 -d i2b2_app03_ASProgression_Data --file-path /home/richard/projects/mssql_github/backup/ --file-per-object --exclude-headers --exclude-use-database')

load_dotenv()

schedule.every(1).minutes.do(job)

try:
    while True:
            schedule.run_pending()
            time.sleep(1)

except KeyboardInterrupt:
    logging.info('Schedule stopped')
