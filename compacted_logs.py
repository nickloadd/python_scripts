import boto3
import datetime
from datetime import date, timedelta

# Create a DynamoDB client
dynamodb = boto3.client('dynamodb')
# Create a Athena client
athena = boto3.client('athena')

# Database to execute the query against
DATABASE = 'PROJECT_NAME'

# Output location for query results
output = 's3://PROJECT_NAME-compacted-logs/'

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_time():
    # Retrieve the item from your table
    response = dynamodb.get_item(
    TableName='PROJECT_NAME-lambda-timestamp',
        Key={
            'pk': {'N': '1'}
        }
    )
    # Get the timestamp from the item
    item = response['Item']
    timestamp = str(item['timestamp']['S'])
    dt = datetime.datetime.strptime(timestamp, '%Y-%m-%d').date()
    print("Last Timestamp in DDB:", dt)
    return dt

def update_time(time: int):
    # Create a new item in your table
    response = dynamodb.put_item(
    TableName='PROJECT_NAME-lambda-timestamp',
        Item={
            'pk': {'N': '1'},
            'timestamp': {'S': time}
        }
    )
    # dt = datetime.datetime.fromtimestamp(time)
    print("Timestamp was updated with value:", time)
    return print("Update result:", response)

def update_compacted_logs(start_date, end_date):
    # dt = datetime.datetime.fromtimestamp(time) 
    for single_date in daterange(start_date, end_date):
        print("Inserting day:", str(single_date))
        year = single_date.strftime("%Y")
        month = single_date.strftime("%m")
        day = single_date.strftime("%d")
        # Query string to execute
        query = f"""
            INSERT INTO PROJECT_NAME.compacted_logs_dev SELECT *
                    FROM PROJECT_NAME.raw_logs_dev
                    WHERE year = '{year}'
                        AND month = '{month}'
                        AND day = '{day}';
                        """
        print("query=",query)
        # Start the query execution
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': DATABASE
            },
            ResultConfiguration={
                'OutputLocation': output
            }
        )
        print("Response:", response)
        print("Successful insert data of " + str(single_date) + " day")
    return response

def lambda_handler(event, context):
    # end_date_ts = int(datetime.datetime.utcnow().timestamp())
    # end_date_dt = datetime.datetime.fromtimestamp(end_date_ts)
    end_date = date.today()
    if (get_time() != end_date):
        update_compacted_logs(get_time(), end_date)
    else:
        raise ValueError("Data is already updated")
    update_time(end_date)
    return None