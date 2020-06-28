# 2020-06-28 Nick Taylor | nick@nicktaylor.co.uk
# Script to parse SmartThings / Bulb energy items from PubSub subscription, pick specific attributes and put them in a BigQuery table

from google.cloud import bigquery
import base64, json, sys, os

def pubsub_to_bigq(event, context):
   pubsub_data = base64.b64decode(event['data']).decode('utf-8')
   pubsub_attribs = event['attributes']
   print("Timestamp: ", pubsub_attribs.get('date'))
   print("Key: ", pubsub_attribs.get('key'))
   print("Value: ", pubsub_attribs.get('value'))

   insert_rows = {}
   for variable in ["date","key","value"]:
      insert_rows[variable] = pubsub_attribs.get(variable)

   print(insert_rows)
   to_bigquery(os.environ['dataset'], os.environ['table'], insert_rows)

def to_bigquery(dataset, table, document):
   bigquery_client = bigquery.Client()
   dataset_ref = bigquery_client.dataset(dataset)
   table_ref = dataset_ref.table(table)
   table = bigquery_client.get_table(table_ref)
   errors = bigquery_client.insert_rows(table, [document])
   if errors != [] :
      print(errors, file=sys.stderr)
