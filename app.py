from flask import Flask, request, jsonify
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO

app = Flask(__name__)

# Azure Blob Storage configuration
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=legitpaystorage;AccountKey=y3gIKRQyZYkbNjdwq6qhxUM5fgVm3dZYgIfo8FvIb/BGAfvVImb5Z4mXm01DyhDYXc6bfO/ah2tT+ASt1inDcA==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "abc-company-data"


# Function to read CSV from Azure
def read_csv_from_azure(blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

    stream = blob_client.download_blob()
    csv_data = stream.readall().decode('utf-8')
    
    df = pd.read_csv(StringIO(csv_data))
    return df

# Function to check for specific account number
def filter_by_account_number(account_number):
    df = read_csv_from_azure("Outliers/data.csv")

    filtered_df = df[
        (df["Bank_Account_Number"] == account_number) & 
        (df['Assignments'].astype(str) == '0')
    ]
    return not filtered_df.empty

def write_df_to_azure(df, blob_name):
    """
    Writes a pandas DataFrame as a CSV file to Azure Blob Storage.
    """
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)


# Flask endpoint
@app.route('/check_account/<account_number>', methods=['GET'])
def check_account(account_number):

    if not account_number:
        return jsonify({"error": "Missing 'account_number' in request body"}), 400

    Outlier = filter_by_account_number(account_number)

    return jsonify(Outlier), 200


@app.route('/check_criticality', methods=['POST'])
def get_account_criticality():
    try:
        data = request.get_json()

        # Extract transaction details from request
        transaction = data.get('transaction')

        if not transaction:
            return jsonify({'error': 'Missing transaction data'}), 400

        account_number = transaction.get('Bank_Account_Number')

        if not account_number:
            return jsonify({'error': 'Missing Bank_Account_Number in transaction'}), 400

        # Read data
        transaction_df = read_csv_from_azure('transactions.csv')
        user_df = read_csv_from_azure('users.csv')

        # Convert to datetime
        transaction_df['Transaction_Timestamp'] = pd.to_datetime(transaction_df['Transaction_Timestamp'])

        # Merge data
        merge_df = transaction_df.merge(user_df, how='inner', on='User_ID')
        # Filter by account number
        account_df = merge_df[merge_df['Bank_Account_Number_x'] == account_number]

        if account_df.empty:
            return jsonify({'criticality': 'Unknown', 'reason': 'No transaction history found'})

        # Get last 10 transactions
        last_10_txns = account_df.sort_values(by='Transaction_Timestamp', ascending=False).iloc[:10]

        # Calculate average and std deviation of invoice amount
        avg_amt = last_10_txns['Invoice_Amount'].mean()
        std_amt = last_10_txns['Invoice_Amount'].std()

        # Calculate refund ratio (guard against division by zero)
        invoice_amt = transaction.get('Invoice_Amount', 0)
        refund_amt = transaction.get('Refund_Amount', 0)
        refund_ratio = refund_amt / invoice_amt if invoice_amt != 0 else 0

        # Flags for new behavior
        is_new_geo = transaction.get('Transaction_Geolocation') not in last_10_txns['Transaction_Geolocation'].values
        is_new_method = transaction.get('Payment_Method') not in last_10_txns['Payment_Method'].values

        # Determine criticality level
        if refund_ratio > 0.8 or invoice_amt > (avg_amt + 3 * std_amt):
            criticality = "High"
        elif is_new_geo or is_new_method or refund_ratio > 0.5:
            criticality = "Medium"
        else:
            criticality = "Low"

        return jsonify({'criticality': criticality})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


def get_transactions_with_risks(date):
    df = read_csv_from_azure("TransactionWRisk.csv")
    filtered_df_wRisk = df[
        (df["Transaction_Date"] == date)
    ]
    return filtered_df_wRisk

# Flask endpoint
@app.route('/fetchtransactionwithrisk/<date>', methods=['GET'])
def fetchTransactionsWRisk(date):
    if not date:
        return jsonify({"error": "Missing not found in request body"}), 400

    Outlier = get_transactions_with_risks(date)

    return jsonify(Outlier.to_dict(orient="records")), 200

if __name__ == '__main__':
    app.run(debug=True)
