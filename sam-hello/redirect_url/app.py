import os, json, boto3

TABLE_NAME = os.environ.get("TABLE_NAME", "url-mappings")
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

def lambda_handler(event, context):
    path_params = (event or {}).get("pathParameters") or {}
    code = path_params.get("shortCode")
    if not code:
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"message": "shortCode is required"})
        }

    resp = table.get_item(Key={"shortCode": code})
    item = resp.get("Item")
    if not item:
        return {
            "statusCode": 404,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"message": "Not Found"})
        }

    return {
        "statusCode": 302,
        "headers": {"Location": item["longUrl"]},
        "body": ""
    }
