import boto3
import pandas as pd
import re
from pdf2image import convert_from_path
import pytesseract
import json

# Initialize the S3 client
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1')

# Set your S3 bucket and file names
bucket_name = 'my-terraform-bucket'
csv_file_key = 'transactions.csv'
pdf_file_key = 'invoice.pdf'

# Download the CSV file from S3 to local
s3.download_file(bucket_name, csv_file_key, '/tmp/transactions.csv')

# Read the CSV file into a pandas DataFrame
csv_data = pd.read_csv('/tmp/transactions.csv')
print("CSV Data Before Cleaning:")
print(csv_data.head())

# Example of CSV data cleaning
# Clean and transform the data
csv_data['transaction_date'] = pd.to_datetime(csv_data['transaction_date'])
# Removing any leading or trailing spaces from column names
csv_data.columns = csv_data.columns.str.strip()

# Handling missing values (for example, replacing NaN with 0 or filling forward)
csv_data.fillna(0, inplace=True)

# If needed, you can convert columns to appropriate data types
csv_data['amount'] = csv_data['amount'].apply(pd.to_numeric, errors='coerce')

# Example: Removing rows where 'Amount' is 0
csv_data = csv_data[csv_data['amount'] != 0]

print("\nCSV Data After Cleaning:")
print(csv_data.head())

# Step 1: Convert cleaned CSV data to Parquet
parquet_file_path = '/tmp/cleaned_transactions.parquet'
csv_data.to_parquet(parquet_file_path, index=False)

# Step 2: Upload the Parquet file to S3
s3.upload_file(parquet_file_path, bucket_name, 'cleaned_transactions.parquet')

print("\nCleaned CSV data uploaded to S3 as Parquet.")

# Download the PDF file from S3 to local
s3.download_file(bucket_name, pdf_file_key, '/tmp/invoice.pdf')

# Convert the PDF to images using pdf2image
pages = convert_from_path('/tmp/invoice.pdf', 300)

# Use pytesseract to extract text from each page image
pdf_text = ""
for page in pages:
    page_text = pytesseract.image_to_string(page)
    pdf_text += page_text

# Example of PDF text cleaning
print("\nExtracted PDF Text Before Cleaning:")
print(pdf_text[:500])  # print a snippet of the text

# Clean extracted text (for example, remove unwanted characters, extra spaces)
clean_pdf_text = re.sub(r'\s+', ' ', pdf_text)  # Remove multiple spaces/newlines
clean_pdf_text = clean_pdf_text.strip()  # Remove leading/trailing spaces

# Example: Extract key information (e.g., invoice number, amount) using regex
invoice_number = re.search(r'Invoice Number: (\d+)', clean_pdf_text)
if invoice_number:
    invoice_number = invoice_number.group(1)

# Example: Extract amounts (assuming they are preceded by a dollar sign)
amounts = re.findall(r'\$\d+(?:\.\d{2})?', clean_pdf_text)

print("\nExtracted Invoice Number:", invoice_number)
print("\nExtracted Amounts:", amounts)

# Structure extracted PDF data in a dictionary for uploading
pdf_data = {
    "invoice_number": invoice_number,
    "amounts": amounts,
    "raw_text": clean_pdf_text
}

# Step 3: Save the extracted PDF data as a JSON file locally
pdf_json_file_path = '/tmp/extracted_pdf_data.json'
with open(pdf_json_file_path, 'w') as json_file:
    json.dump(pdf_data, json_file)

# Step 4: Upload the JSON file to S3
s3.upload_file(pdf_json_file_path, bucket_name, 'extracted_pdf_data.json')

print("\nExtracted PDF data uploaded to S3 as JSON.")
