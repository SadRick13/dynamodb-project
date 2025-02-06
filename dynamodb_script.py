import boto3
import os
from dotenv import load_dotenv

# Load AWS credentials from .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

# Initialize DynamoDB client
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# Connect to Users table
table = dynamodb.Table("Users")

# Function for printing all items in the table
def print_table(Table = table):
    response = Table.scan()
    print("Users Table:")
    for item in response["Items"]:
        print(item)

# Insert an item
table.put_item(
    Item={"user_id": "u005", "name": "Emily", "age": 32, "city": "San Francisco"}
)
print("Item inserted successfully!")


# Insert item with different attributes

table.put_item(
    Item={"user_id": "u006", "name": "Francois", "age": "unknown", "city": "Paris", "job": "Engineer"}
)
print("Item inserted successfully!")

# Retrieve the item
response = table.get_item(Key={"user_id": "u006"})
print("Retrieved User:", response["Item"])

# Print the table
print_table(table)
