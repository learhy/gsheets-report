#!/usr/bin/env python

import argparse
import psycopg2
import traceback
import base64
import httplib2
import csv
import os
import sys
import gflags
import hyou
import hyou.client
import oauth2client.client

parser = argparse.ArgumentParser(description='Runs specified query then sends query to email')
parser.add_argument('--host', help='ip address of db host', default='dbhost')
parser.add_argument('--user', help='db username', default='dbuser')
parser.add_argument('--dbpass', help='dp passwd', default='db_passwd')
parser.add_argument('queryfile', help='path to plaintext file that includes the sql formatted query')
parser.add_argument('--db', help="database to be queried", default='db_name')
parser.add_argument('--port', help="port that pgsql server is running on", default='5434')
args = parser.parse_args()

CREDENTIAL_PATH = os.path.join(os.environ['HOME'], '.hyou.credential.json')
TEST_CLIENT_ID = '958069810280-th697if59r9scrf1qh0sg6gd9d9u0kts.apps.googleusercontent.com'
TEST_CLIENT_SECRET = '5nlcvd54WycOd8h8w7HD0avT'


FLAGS = gflags.FLAGS

gflags.DEFINE_bool('authenticate', False, '')
gflags.DEFINE_string('client_id', TEST_CLIENT_ID, '')
gflags.DEFINE_string('client_secret', TEST_CLIENT_SECRET, '')
gflags.MarkFlagAsRequired('client_id')
gflags.MarkFlagAsRequired('client_secret')


def load_sheet(path):
    with open(path, 'rb') as f:
        reader = csv.reader(f)
        return list(reader)

def upload_main(filename):
    if len(filename) != 2:
        return __doc__

    path = filename

    sheet = load_sheet(path)

    try:
        collection = hyou.login(json_path=CREDENTIAL_PATH)
    except Exception:
        return ('Your credential is missing, expired or invalid.'
                'Please authenticate again by --authenticate.')

    title = os.path.basename(path).decode('utf-8')
    spreadsheet = collection.create_spreadsheet(
        title, rows=len(sheet), cols=len(sheet[0]))

    with spreadsheet[0] as worksheet:
        for srow, trow in zip(sheet, worksheet):
            for i, value in enumerate(srow):
                trow[i] = value.decode('utf-8')

    print spreadsheet.url


def readQuery():
	with open(queryfile, 'r') as myfile:
		query = myfile.read()
	return(query)

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def runQuery(query):
	conn_string = "host={} dbname={} user={} password={} port={}".format(host, dbname, user, dbpass, port)
	try:
		conn = psycopg2.connect(conn_string)
	except psycopg2.Error as e:
		print("Unable to connect to the database.")
	try:
		curs = conn.cursor()
		outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
		with open('resultsfile.csv', 'w') as f:
			curs.copy_expert(outputquery, f)
		# Return the path the the CSV file
		return(f.name)
		conn.close() 
	except psycopg2.Error as e:
		print traceback.format_exc()


if __name__ == "__main__":

	# Set up command line arguments	
	conn = None
	queryfile = args.queryfile
	host=args.host
	dbname=args.db
	user=args.user
	dbpass=args.dbpass
	port=args.port
	resultsfile = 'resultsfile.csv'
	
	# do the things
	query = readQuery()
	filename = runQuery(query)
# 	sendMail(sender, recipient, subject)
	

		
		
