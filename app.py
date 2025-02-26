from flask import Flask, request, jsonify
import boto3

app = Flask(__name__)  # Create Flask app

# Initialize AWS DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Users")


# 🟢 Route 1: Add a User (Equivalent to `python cli_tool.py add --user_id u100 --name Alice --city Berlin`)
@app.route("/add_user", methods=["POST"])
def add_user():
    data = request.json  # Read JSON payload from the user
    user_id = data.get("user_id")
    name = data.get("name")
    age = data.get("age")
    city = data.get("city")

    # Validate input
    if not user_id or not name or not city:
        return jsonify({"error": "Missing required fields"}), 400  # HTTP 400 = Bad Request

    # Add user to DynamoDB
    table.put_item(Item={"user_id": user_id, "name": name, "age": age, "city": city})
    return jsonify({"message": f"User {name} added successfully!"})



# 🔍 Route 2: Get a User (Equivalent to `python cli_tool.py get --user_id u100`)
@app.route("/get_user", methods=["GET"])
def get_user():
    user_id = request.args.get("user_id")  # Read query parameter

    if not user_id:
        return jsonify({"error": "Missing user_id parameter"}), 400

    # Fetch user from DynamoDB
    response = table.get_item(Key={"user_id": user_id})
    if "Item" in response:
        return jsonify(response["Item"])
    else:
        return jsonify({"error": "User not found"}), 404
    


# ❌ Route 3: Delete a User (Equivalent to `python cli_tool.py delete --user_id u100`)
@app.route("/delete_user", methods=["DELETE"])
def delete_user():
    user_id = request.args.get("user_id")

    if not user_id:
        return jsonify({"error": "Missing user_id parameter"}), 400

    # Delete user from DynamoDB
    table.delete_item(Key={"user_id": user_id})
    return jsonify({"message": f"User {user_id} deleted successfully!"})


# Run the API locally
if __name__ == "__main__":
    app.run(debug=True)