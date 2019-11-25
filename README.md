# MS SQL Github

Scripts to backup MS SQL changes to github.

## How to Run

1. Copy `example.env` file to `.env` and replace the values with the proper values.
2. To run the scripting immediately, run the command `python run.py --run`
3. To schedule the scripting, run the command `python run.py`

## Environment Variables

The `DATABASES` environment variable is a JSON string of a list of databases.
