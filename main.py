import boto3
import pandas as pd
import re
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

# Example of structuring the cleaned PDF text (if needed for later processing)
# You could now integrate this data back into your system or continue cleaning.

# You can now upload the transformed CSV and processed text back to S3 or other systems.
