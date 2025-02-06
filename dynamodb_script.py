import boto3
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

# Function for printing all items in the table
def print_table(table,max_items=100):
    """
    Prints items from DynamoDB table

    Args:
        table (boto3.resource.Table): DynamoDB table object
        max_items (int): Maximum number of items to print
    Raises:
        ValueError: If the provided 'table' is not a valid DynamoDB Table object.
        Exception: For any unexpected errors during the scan operation.
    """
    if not hasattr(table, 'scan'):
        raise ValueError("Invalid table object. Make sure you're passing a DynamoDB Table resource.")
    
        # 2️⃣ Inform User About Large Tables
    try:
        # Efficiently get the total item count
        item_count = table.item_count
        if item_count is None:  # In case item_count isn't populated
            item_count = table.scan(Select='COUNT')["Count"]

        if item_count > max_items:
            print(f"⚠️  Warning: The table contains {item_count} items. Displaying only the first {max_items} items.")
    except (BotoCoreError, ClientError) as e:
        print(f"Error fetching item count: {e}")
        return

    # 3️⃣ Scanning the Table (Without Pagination Handling)
    try:
        response = table.scan(Limit=max_items)
        items = response.get("Items", [])

        if not items:
            print("The table is empty.")
        else:
            print("Users Table:")
            for item in items:
                print(item)

            print(f"\n✅ Displayed {len(items)} items from the table.")

    except (BotoCoreError, ClientError) as e:
        print(f"Error scanning the table: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")



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
print_table(table, max_items=10)
