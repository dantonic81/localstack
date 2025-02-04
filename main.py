import boto3
import pandas as pd
from pdf2image import convert_from_path
import pytesseract

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
print(csv_data.head())

# Download the PDF file from S3 to local
s3.download_file(bucket_name, pdf_file_key, '/tmp/invoice.pdf')

# Convert the PDF to images using pdf2image
pages = convert_from_path('/tmp/invoice.pdf', 300)

# Use pytesseract to extract text from each page image
pdf_text = ""
for page in pages:
    page_text = pytesseract.image_to_string(page)
    pdf_text += page_text

# Print the extracted text from the PDF
print(pdf_text)
