# 1. Delete the existing table
aws dynamodb delete-table \
    --table-name content-curator-metadata \
    --region eu-north-1

# 2. Wait for the table to be deleted (this can take a minute)
aws dynamodb wait table-not-exists \
    --table-name content-curator-metadata \
    --region eu-north-1

# 3. Create a new table with the updated schema
aws dynamodb create-table \
    --table-name content-curator-metadata \
    --attribute-definitions AttributeName=guid,AttributeType=S \
    --key-schema AttributeName=guid,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region eu-north-1

# 4. Wait for the table to be created and available
aws dynamodb wait table-exists \
    --table-name content-curator-metadata \
    --region eu-north-1

echo "DynamoDB table recreated successfully"