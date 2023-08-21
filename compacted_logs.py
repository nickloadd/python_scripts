import boto3
import datetime

# Create a DynamoDB client
dynamodb = boto3.client('dynamodb')
# Create a Athena client
athena = boto3.client('athena')

# Database to execute the query against
DATABASE = 'PROJECT_NAME'

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
    timestamp = int(item['timestamp']['N'])
    dt = datetime.datetime.fromtimestamp(timestamp)
    print("Last Timestamp in DDB:", dt)
    return timestamp

def update_time(time: int):
    # Get the current timestamp
    timestamp = time
    # Create a new item in your table
    response = dynamodb.put_item(
    TableName='PROJECT_NAME-lambda-timestamp',
        Item={
            'pk': {'N': '1'},
            'timestamp': {'N': str(timestamp)}
        }
    )
    dt = datetime.datetime.fromtimestamp(timestamp)
    print("Timestamp was updated with value:", dt)

def update_compacted_logs(time: int):
    dt = datetime.datetime.fromtimestamp(time)
    year = dt.year
    month = dt.month
    day = dt.day
    hour = dt.hour
    # Query string to execute
    query = f"""INSERT INTO PROJECT_NAME.compacted_logs_dev SELECT 
        COL1,
        COL2,
        COL3,
        COL4,
        COL5,
        year,
        month,
        day,
        hour
            FROM PROJECT_NAME.raw_logs_dev
            WHERE year >= '{year}'
                AND month >= '{month}'
                AND day >= '{day}'
                AND hour >= '{hour}'
                AND time >= CAST('{dt}' AS timestamp(3))"""

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
    return None

def lambda_handler(event, context):
    last_time_tmp = int(datetime.datetime.utcnow().timestamp())
    update_compacted_logs(get_time())
    update_time(last_time_tmp)
    return None