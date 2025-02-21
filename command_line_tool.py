import boto3
import json
import argparse
import sys
from botocore.exceptions import BotoCoreError, ClientError
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



def add_user(user_id, name, age, city):
    response = table.put_item(
        Item={
            "user_id": user_id,
            "name": name,
            "age": age,
            "city": city
        }
    )
    print(f"User {name} added successfully.")

def delete_user(user_id):
    response = table.delete_item(
        Key={
            "user_id": user_id
        }
    )
    print(f"User {user_id} deleted successfully!")


def get_user(user_id=None, name=None, city=None, age=None):
    if user_id:
        response = table.get_item(
            Key={
                "user_id": user_id
            }
        )
        item = response.get("Item", None)
        if item:
            print(item)
        else:
            print(f"User {user_id} not found.")
    else:
        filter_expression = []
        expression_values = {}
        if name:
            filter_expression.append("name = :name")
            expression_values[":name"] = name
        if city:
            filter_expression.append("city = :city")
            expression_values[":city"] = city
        if age:
            filter_expression.append("age = :age")
            expression_values[":age"] = age

        if not filter_expression:
            print("⚠️ Please provide at least one attribute to search.")
            return

        filter_expression_str = " AND ".join(filter_expression)
        response = table.scan(
            FilterExpression=filter_expression_str,
            ExpressionAttributeNames={"#name": "name", "#city": "city", "#age": "age"},
            ExpressionAttributeValues=expression_values
        )
        users = response.get("Items", [])
        if users:
            print(f"✅ Found {len(users)} matching users:")
            print(json.dumps(users, indent=4))
        else:
            print("⚠️ No matching users found.")


# Set up argument parsing with subcommands
parser = argparse.ArgumentParser(description="DynamoDB User Manager")
subparsers = parser.add_subparsers(dest="command", required=True)

# Add command
add_parser = subparsers.add_parser("add", help="Add a new user")
add_parser.add_argument("--user_id", required=True, help="User ID")
add_parser.add_argument("--name", required=True, help="User name")
add_parser.add_argument("--age", required=False, type=int, help="User age")
add_parser.add_argument("--city", required=True, help="User city")

# Get command
get_parser = subparsers.add_parser("get", help="Retrieve users")
get_parser.add_argument("--user_id", help="Search by user ID")
get_parser.add_argument("--name", help="Search by name")
get_parser.add_argument("--city", help="Search by city")
get_parser.add_argument("--age", type=int, help="Search by age")

# Delete command
delete_parser = subparsers.add_parser("delete", help="Delete a user")
delete_parser.add_argument("--user_id", required=True, help="User ID to delete")

args = parser.parse_args()


# Execute the correct function based on the subcommand
if args.command == "add":
    print("DEBUG: Arguments Received:", args)  # Debugging
    add_user(args.user_id, args.name, args.age, args.city)
elif args.command == "get":
    get_user(args.user_id, args.name, args.city, args.age)
elif args.command == "delete":
    delete_user(args.user_id)