import boto3
import pandas as pd
import re
from pdf2image import convert_from_path
import pytesseract
import json
import logging
from concurrent.futures import ThreadPoolExecutor

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the S3 client
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1')

# Set your S3 bucket names and file keys
landing_bucket_name = 'landing'  # Staging area
prod_bucket_name = 'prod'  # Production data lake
csv_file_key = 'transactions.csv'
pdf_file_key = 'invoice.pdf'


def download_file_from_s3(bucket_name, file_key, local_path):
    """Download a file from S3 to local."""
    try:
        s3.download_file(bucket_name, file_key, local_path)
        logger.info(f"Downloaded {file_key} from {bucket_name} to {local_path}")
    except Exception as e:
        logger.error(f"Error downloading {file_key} from {bucket_name}: {e}")
        raise


def clean_csv_data(csv_data):
    """Clean and transform the CSV data."""
    logger.info("Cleaning CSV data...")
    csv_data['transaction_date'] = pd.to_datetime(csv_data['transaction_date'])
    csv_data.columns = csv_data.columns.str.strip()  # Remove leading/trailing spaces from column names
    csv_data.fillna(0, inplace=True)  # Handle missing values
    csv_data['amount'] = csv_data['amount'].apply(pd.to_numeric, errors='coerce')  # Convert amount to numeric
    csv_data = csv_data[csv_data['amount'] != 0]  # Remove rows where amount is 0
    return csv_data


def convert_csv_to_parquet(csv_data, parquet_file_path):
    """Convert cleaned CSV data to Parquet format."""
    logger.info(f"Converting CSV to Parquet and saving to {parquet_file_path}...")
    csv_data.to_parquet(parquet_file_path, index=False)


def upload_file_to_s3(local_file_path, bucket_name, file_key):
    """Upload a file from local to S3."""
    try:
        s3.upload_file(local_file_path, bucket_name, file_key)
        logger.info(f"Uploaded {local_file_path} to {bucket_name} as {file_key}")
    except Exception as e:
        logger.error(f"Error uploading {local_file_path} to {bucket_name}: {e}")
        raise


def extract_text_from_pdf(pdf_file_path):
    """Convert PDF to text using pytesseract."""
    logger.info("Extracting text from PDF...")
    pages = convert_from_path(pdf_file_path, 300)
    with ThreadPoolExecutor() as executor:
        pdf_texts = list(executor.map(pytesseract.image_to_string, pages))
    return ''.join(pdf_texts)


def clean_pdf_text(pdf_text):
    """Clean and extract useful information from the PDF text."""
    clean_text = re.sub(r'\s+', ' ', pdf_text).strip()  # Remove multiple spaces/newlines and leading/trailing spaces
    return clean_text


def extract_invoice_data(clean_text):
    """Extract invoice number and amounts from the cleaned PDF text."""
    invoice_number = re.search(r'Invoice Number: (\d+)', clean_text)
    invoice_number = invoice_number.group(1) if invoice_number else None
    amounts = re.findall(r'\$\d+(?:\.\d{2})?', clean_text)
    return invoice_number, amounts


def main():
    # Step 1: Download and process CSV data
    try:
        download_file_from_s3(landing_bucket_name, csv_file_key, '/tmp/transactions.csv')
        csv_data = pd.read_csv('/tmp/transactions.csv')
        logger.info("CSV Data Before Cleaning:")
        logger.info(csv_data.head())

        cleaned_csv_data = clean_csv_data(csv_data)
        logger.info("\nCSV Data After Cleaning:")
        logger.info(cleaned_csv_data.head())

        # Step 2: Convert cleaned CSV data to Parquet and upload to prod data lake
        parquet_file_path = '/tmp/cleaned_transactions.parquet'
        convert_csv_to_parquet(cleaned_csv_data, parquet_file_path)
        upload_file_to_s3(parquet_file_path, prod_bucket_name, 'cleaned_transactions.parquet')

    except Exception as e:
        logger.error(f"Error processing CSV data: {e}")
        return

    # Step 3: Download and process PDF data
    try:
        download_file_from_s3(landing_bucket_name, pdf_file_key, '/tmp/invoice.pdf')
        pdf_text = extract_text_from_pdf('/tmp/invoice.pdf')
        logger.info("\nExtracted PDF Text Before Cleaning:")
        logger.info(pdf_text[:500])  # print a snippet of the text

        clean_text = clean_pdf_text(pdf_text)

        # Step 4: Extract invoice data and upload to prod data lake
        invoice_number, amounts = extract_invoice_data(clean_text)
        logger.info("\nExtracted Invoice Number: %s", invoice_number)
        logger.info("\nExtracted Amounts: %s", amounts)

        pdf_data = {
            "invoice_number": invoice_number,
            "amounts": amounts,
            "raw_text": clean_text
        }

        pdf_json_file_path = '/tmp/extracted_pdf_data.json'
        with open(pdf_json_file_path, 'w') as json_file:
            json.dump(pdf_data, json_file)

        upload_file_to_s3(pdf_json_file_path, prod_bucket_name, 'extracted_pdf_data.json')

    except Exception as e:
        logger.error(f"Error processing PDF data: {e}")
        return


if __name__ == "__main__":
    main()
