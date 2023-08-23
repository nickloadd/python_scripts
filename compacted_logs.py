import boto3
import botocore
import datetime
import time
from datetime import date, timedelta

# Database to execute the query against
DATABASE = 'PROJECT_NAME'

# Output location for query results
output_bucket = 'PROJECT_NAME-compacted-logs'

# Generate every date in range
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# Get start date from DDB
def get_start_date():
    # Create a DynamoDB client
    dynamodb = boto3.client('dynamodb')
    # Retrieve the item from table
    try:
        response = dynamodb.get_item(
        TableName='PROJECT_NAME-lambda-date',
            Key={
                'env': {'S': 'prod'}
            }
        )
    except botocore.exceptions.ClientError as error:        
        print('Error Message: {}'.format(error.response['Error']['Message']))
        raise error
    except botocore.exceptions.ParamValidationError as error:
        raise ValueError('The parameters you provided are incorrect: {}'.format(error))
    # Get the date from the item
    item = response['Item']
    start_date = datetime.datetime.strptime(item['date']['S'], '%Y-%m-%d').date()
    return start_date

# Update date in DDB
def update_date(last_update_date):
    # Create a DynamoDB client
    dynamodb = boto3.client('dynamodb')
    # Create a new item in table
    try:
        response = dynamodb.put_item(
        TableName='PROJECT_NAME-lambda-date',
            Item={
                'env': {'S': 'prod'},
                'date': {'S': last_update_date}
            }
        )
    except botocore.exceptions.ClientError as error:
        print('Error Message: {}'.format(error.response['Error']['Message']))
        raise error
    except botocore.exceptions.ParamValidationError as error:
        raise ValueError('The parameters you provided are incorrect: {}'.format(error))
    return print("Date in DDB was updated with value:", last_update_date)

# Run athena query
def run_athena_query(database, query):
    # Create a Athena client
    athena = boto3.client('athena')
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f's3://{output_bucket}/query-results/'
        }
    )
    return response['QueryExecutionId']

# Wait completion of query
def wait_for_query_to_complete(athena, query_execution_id, wait_interval=5):
    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']

        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break

        time.sleep(wait_interval)

    if state == 'FAILED':
        error_message = response['QueryExecution']['Status'].get('StateChangeReason')
        print(f"Query failed: {error_message}")

    return state

# Get query result
def get_query_result(query_execution_id):
    athena = boto3.client('athena')
    wait_for_query_to_complete(athena, query_execution_id)
    response = athena.get_query_results(
        QueryExecutionId=query_execution_id
    )
    return response

# Insert into compacted logs
def update_compacted_logs(start_date, end_date):
    print("Last date in DDB:", start_date)
    for single_date in daterange(start_date, end_date):
        print("Inserting day:", str(single_date))
        year = single_date.strftime("%Y")
        month = single_date.strftime("%m")
        day = single_date.strftime("%d")
        # Build query string to execute
        query = f"""
            INSERT INTO PROJECT_NAME.compacted_logs_prod SELECT *
                    FROM PROJECT_NAME.raw_logs_prod
                    WHERE year = '{year}'
                        AND month = '{month}'
                        AND day = '{day}';
                        """
        # Start the query execution
        query_execution_id = run_athena_query(DATABASE, query)
        get_query_result(query_execution_id)
        print("Inserted day:", str(single_date))

def lambda_handler(event, context):
    end_date = date.today()
    if (get_start_date() < end_date):
        update_compacted_logs(get_start_date(), end_date)
    else:
        raise ValueError("Data is already updated")
    update_date(str(end_date))
    return None