from flask import Flask, request, jsonify, Response
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO
# import asyncio
# from semantic_kernel.agents import ChatCompletionAgent, ChatHistoryAgentThread
# from semantic_kernel.connectors.ai.azure_ai_inference import AzureAIInferenceChatCompletion, AzureAIInferenceChatPromptExecutionSettings
# from semantic_kernel.connectors.ai import FunctionChoiceBehavior
# from semantic_kernel.core_plugins.time_plugin import TimePlugin
# from semantic_kernel.functions import KernelArguments, kernel_function
# from semantic_kernel.kernel import Kernel
# from typing import Annotated

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

        isOutlier = filter_by_account_number(account_number)
        if isOutlier:
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
                status = "Hold"
            elif is_new_geo or is_new_method or refund_ratio > 0.5:
                criticality = "Medium"
                status = "Open"
            else:
                criticality = "Low"
                status = "Open"
            transaction['Risk'] = criticality
            transaction['Status'] = status

            df = read_csv_from_azure("TransactionWRisk.csv")
            df.loc[len(df)] = transaction
            write_df_to_azure(df, "TransactionWRisk.csv")
            return 'Record Added!!'
        else:
            raise ValueError("Record entry failed!!")
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def get_transactions_with_risks(date):
    df = read_csv_from_azure("TransactionWRisk.csv")
    filtered_df_wRisk = df[
        (df["Transaction_Date"] == date)
    ]
    return filtered_df_wRisk

def get_transactions_with_transactionNum(tran_number):
    df = read_csv_from_azure("TransactionWRisk.csv")
    filtered_df_wRisk = df[
        (df["Transaction_ID"] == tran_number)
    ]
    return filtered_df_wRisk
# Flask endpoint
@app.route('/fetchtransactionfromtrannum/<tran_number>', methods=['GET'])
def fetchTransactionsWRisk(tran_number):
    if not tran_number:
        return jsonify({"error": "Missing not found in request body"}), 400

    Outlier = get_transactions_with_transactionNum(tran_number)
    if Outlier.empty:
        return "No such transaction found."
    outlier_string = str(Outlier.to_dict(orient="records"))
    return outlier_string


@app.route('/fetchtransactionwithrisk/<date>', methods=['GET'])
def fetchTransactionsWRiskInroute(date):
    if not date:
        return jsonify({"error": "Missing not found in request body"}), 400

    Outlier = get_transactions_with_risks(date)
    outlier_string = str(Outlier.to_dict(orient="records"))
    return outlier_string


# Function to update the transaction status
def update_transaction_status(transaction_id, status):
    df = read_csv_from_azure("TransactionWRisk.csv")

    # Find the row with the matching transaction ID and update the status
    transaction_index = df[df["Transaction_ID"] == transaction_id].index
    if transaction_index.empty:
        return False  # Transaction ID not found
    
    df.loc[transaction_index, "Status"] = status  # Update the status
    write_df_to_azure(df, "TransactionWRisk.csv")  # Upload the updated CSV back to Azure
    
    return True

@app.route('/update_transaction_status/<transaction_id>', methods=['POST'])
def update_status(transaction_id):
    try:
        # Get data from the request
        data = request.get_json()
        status = data.get('status')

        if not transaction_id or not status:
            return jsonify({'error': 'Transaction ID and Status are required'}), 400

        # Update the transaction status in the Azure Blob CSV file
        blob_name = 'transactions.csv'  # Specify the CSV file name here
        updated = update_transaction_status(transaction_id, status)

        if not updated:
            return {'error': 'Transaction ID not found'}

        return f'Status of transaction {transaction_id} updated to {status}'

    except Exception as e:
        return jsonify({'error': str(e)}), 500


#''''''''''''''''''''''

# model_id = "gpt-4o-mini"
# endpoint = "https://legitpay1-openai-8a78.openai.azure.com/openai/deployments/gpt-4o-mini"
# api_key = "aec374826400450897b9ce3ee4b6a794"

# class AgentPlugins:
#     @kernel_function(description="Get the metrics for a specific transaction.")
#     def get_account_metrics(self,trx_id: str) -> Annotated[str, "The metrics for the specified Transaction."]:
#         transactions_df = read_csv_from_azure("transactions.csv")
#         df = transactions_df[transactions_df['Transaction_ID'] == trx_id]

#         if df.empty:
#             return f"❌ No data found for Transaction {trx_id}."

#         invoice_sum = df['Invoice_Amount'].sum()
#         refund_sum = df['Refund_Amount'].sum()

#         avg_invoice = df['Invoice_Amount'].mean()
#         avg_refund = df['Refund_Amount'].mean()
#         refund_to_invoice_ratio = refund_sum / invoice_sum if invoice_sum != 0 else 0.0
#         avg_time_to_refund = df['Time_Since_Invoice'].mean()

#         if pd.isna(avg_invoice) and pd.isna(avg_refund) and pd.isna(avg_time_to_refund):
#             return f"⚠️ No meaningful metrics found for account {trx_id}."

#         return (
#             f"📊 Metrics for Transaction: {trx_id}\n"
#             f"- Bank Account Number: {df['Bank_Account_Number'].iloc[0]}\n"
#             f"- Average Invoice Amount: ${avg_invoice:.2f}\n"
#             f"- Average Refund Amount: ${avg_refund:.2f}\n"
#             f"- Refund to Invoice Ratio: {refund_to_invoice_ratio:.4f}\n"
#             f"- Average Time to Refund: {avg_time_to_refund:.2f} days"
#         )
    
#     @kernel_function(description="Get user data for a specific bank account.")
#     def get_user_data(self, bank_account: str) -> Annotated[str, "User details associated with the bank account."]:
#         users_df = read_csv_from_azure('users.csv')
#         user_row = users_df[users_df['Bank_Account_Number'] == bank_account]

#         if user_row.empty:
#             return f"❌ No user found for bank account {bank_account}."

#         user = user_row.iloc[0]
#         return (
#             f"👤 User Information for Bank Account: {bank_account}\n"
#             f"- User ID: {user.name}\n"
#             f"- Account Age: {user['Account_Age']} years\n"
#             f"- Location: {user['User_Geolocation']}"
#         )
#     @kernel_function(description="Get transaction details for a specific transaction id.")
#     def get_transaction_details(self, trx_id: str) -> Annotated[str, "Transaction details for the specified transaction id."]:
#         transactions_df = read_csv_from_azure('TransactionsWRisk.csv')
#         transaction_row = transactions_df[transactions_df['Transaction_ID'] == trx_id]

#         if transaction_row.empty:
#             return f"❌ No transaction found for transaction id {trx_id}."

#         transaction = transaction_row.iloc[0]
#         return (
#             f"📝 Transaction Details for Transaction ID: {trx_id}\n"
#             f"- Bank Account Number: {transaction['Bank_Account_Number']}\n"
#             f"- Invoice Amount: ${transaction['Invoice_Amount']}\n"
#             f"- Refund Amount: ${transaction['Refund_Amount']}\n"
#             f"- Time Since Invoice: {transaction['Time_Since_Invoice']} days"
#         )

# @app.route('/fetchfraudulentreason/<trx_id>', methods=['GET'])
# def fetchfraudulentreason(trx_id):
#     async def run_agent():
#         kernel = Kernel()
#         chat_completion = AzureAIInferenceChatCompletion(ai_model_id=model_id, api_key=api_key, endpoint=endpoint)
#         kernel.add_plugin(AgentPlugins())
#         kernel.add_plugin(TimePlugin())
#         kernel.add_service(chat_completion)

#         settings = AzureAIInferenceChatPromptExecutionSettings()
#         settings.function_choice_behavior = FunctionChoiceBehavior.Auto()

#         agent = ChatCompletionAgent(
#             kernel=kernel,
#             name='Agent2',
#             instructions="You are a fraud analyzer that gives reason as to why the transaction is fraudulent. Use all tools necessary.",
#             arguments=KernelArguments(settings=settings)
#         )

#         thread: ChatHistoryAgentThread = None

#         response_text = ""
#         async for response in agent.invoke(messages=trx_id, thread=thread):
#             thread = response.thread
#             if response.items:
#                 response_text += response.items[0].text

#         return response_text
#     response = asyncio.run(run_agent())
    
#     return response

#''''''''''''''''''''''







if __name__ == '__main__':
    app.run(debug=True)
