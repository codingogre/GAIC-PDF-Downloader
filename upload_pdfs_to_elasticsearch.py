#!/usr/bin/env python3
"""
Script to upload PDFs to Elasticsearch with attachment processor pipeline.
This script:
1. Iterates over PDFs in the great_american_pdfs folder
2. Base64 encodes the content
3. Creates an ingest pipeline with attachment processor
4. Creates an index with mapping including copy_to field for semantic_text
5. Uploads the PDFs to Elasticsearch
"""

import os
import base64
import json
from pathlib import Path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import hashlib
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Elasticsearch configuration
ES_URL = os.getenv("ES_URL")
API_KEY = os.getenv("API_KEY")
INDEX_NAME = "great-american-insurance-pdfs"
PIPELINE_NAME = "pdf-attachment-pipeline"
PDF_FOLDER = "great_american_pdfs"


def create_es_client():
    """Create and return Elasticsearch client."""
    client = Elasticsearch(
        ES_URL,
        api_key=API_KEY,
        verify_certs=True
    )

    # Test connection
    if client.ping():
        print("✓ Connected to Elasticsearch")
        print(f"  Cluster: {client.info()['cluster_name']}")
        print(f"  Version: {client.info()['version']['number']}")
    else:
        raise Exception("Failed to connect to Elasticsearch")

    return client


def create_ingest_pipeline(client):
    """Create the ingest pipeline with attachment processor."""
    pipeline_body = {
        "description": "Extract PDF attachment information",
        "processors": [
            {
                "attachment": {
                    "field": "data",
                    "target_field": "attachment",
                    "indexed_chars": -1,
                    "ignore_missing": True
                }
            },
            {
                "remove": {
                    "field": "data",
                    "ignore_missing": True
                }
            }
        ]
    }

    try:
        client.ingest.put_pipeline(
            id=PIPELINE_NAME,
            description=pipeline_body["description"],
            processors=pipeline_body["processors"]
        )
        print(f"✓ Created ingest pipeline: {PIPELINE_NAME}")
    except Exception as e:
        print(f"! Pipeline creation note: {e}")


def create_index_with_mapping(client):
    """Create index with mapping including copy_to field for semantic_text."""

    # Delete index if it exists (for clean slate)
    if client.indices.exists(index=INDEX_NAME):
        response = input(f"Index '{INDEX_NAME}' already exists. Delete and recreate? (y/n): ")
        if response.lower() == 'y':
            client.indices.delete(index=INDEX_NAME)
            print(f"✓ Deleted existing index: {INDEX_NAME}")
        else:
            print(f"! Using existing index: {INDEX_NAME}")
            return

    mapping = {
        "settings": {
            "index.default_pipeline": PIPELINE_NAME
        },
        "mappings": {
            "properties": {
                "filename": {
                    "type": "keyword"
                },
                "file_path": {
                    "type": "text"
                },
                "file_size": {
                    "type": "long"
                },
                "upload_timestamp": {
                    "type": "date"
                },
                "attachment": {
                    "properties": {
                        "content": {
                            "type": "text",
                            "copy_to": "semantic_content"
                        },
                        "title": {
                            "type": "text"
                        },
                        "name": {
                            "type": "text"
                        },
                        "author": {
                            "type": "text"
                        },
                        "keywords": {
                            "type": "text"
                        },
                        "date": {
                            "type": "date"
                        },
                        "content_type": {
                            "type": "keyword"
                        },
                        "content_length": {
                            "type": "long"
                        },
                        "language": {
                            "type": "keyword"
                        }
                    }
                },
                "semantic_content": {
                    "type": "semantic_text",
                    "inference_id": ".elser-2-elastic"
                }
            }
        }
    }

    try:
        client.indices.create(
            index=INDEX_NAME,
            settings=mapping["settings"],
            mappings=mapping["mappings"]
        )
        print(f"✓ Created index: {INDEX_NAME}")
        print(f"  - Default pipeline: {PIPELINE_NAME}")
        print(f"  - Mapping includes copy_to field: attachment.content -> semantic_content")
    except Exception as e:
        print(f"✗ Error creating index: {e}")
        raise


def get_pdf_files(folder_path):
    """Get all PDF files from the specified folder."""
    pdf_path = Path(folder_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    pdf_files = list(pdf_path.glob("*.pdf"))
    print(f"✓ Found {len(pdf_files)} PDF files in {folder_path}")
    return pdf_files


def encode_pdf_to_base64(file_path):
    """Read PDF file and encode it to base64."""
    with open(file_path, 'rb') as pdf_file:
        return base64.b64encode(pdf_file.read()).decode('utf-8')


def generate_document_id(filename):
    """Generate a consistent document ID from filename for idempotency."""
    # Use filename without extension as base, make it URL-safe
    doc_id = filename.replace('.pdf', '').replace(' ', '-').lower()
    return doc_id


def generate_documents(pdf_files):
    """Generate documents for bulk upload with unique IDs for idempotency."""
    from datetime import datetime, timezone

    for pdf_file in pdf_files:
        try:
            # Base64 encode the PDF content
            pdf_content = encode_pdf_to_base64(pdf_file)

            # Generate consistent document ID for idempotency
            doc_id = generate_document_id(pdf_file.name)

            # Create document with explicit ID
            doc = {
                "_index": INDEX_NAME,
                "_id": doc_id,
                "_source": {
                    "filename": pdf_file.name,
                    "file_path": str(pdf_file.absolute()),
                    "file_size": pdf_file.stat().st_size,
                    "upload_timestamp": datetime.now(timezone.utc).isoformat(),
                    "data": pdf_content
                }
            }

            yield doc

        except Exception as e:
            print(f"✗ Error processing {pdf_file.name}: {e}")


def upload_pdfs(client, pdf_files):
    """Upload PDFs to Elasticsearch using bulk API with idempotency."""
    print(f"\nUploading {len(pdf_files)} PDFs to Elasticsearch...")
    print("(Using document IDs for idempotent uploads - re-running is safe)")

    success_count = 0
    error_count = 0
    errors = []

    try:
        # Use streaming_bulk helper for efficient upload with progress tracking
        for ok, response in streaming_bulk(
            client,
            generate_documents(pdf_files),
            chunk_size=10,
            request_timeout=60,
            raise_on_error=False
        ):
            if ok:
                success_count += 1
                # Extract action type (index/create/update)
                action = list(response.keys())[0]
                doc_id = response[action]['_id']
                print(f"  ✓ Uploaded: {doc_id} ({success_count}/{len(pdf_files)})")
            else:
                error_count += 1
                errors.append(response)
                print(f"  ✗ Failed: {response}")

        print(f"\n✓ Upload complete!")
        print(f"  Success: {success_count}")
        print(f"  Errors: {error_count}")

        if errors:
            print(f"\nError details:")
            for err in errors[:5]:  # Show first 5 errors
                print(f"  {err}")

    except Exception as e:
        print(f"✗ Bulk upload error: {e}")
        raise


def verify_documents(client):
    """Verify that documents were uploaded successfully."""
    client.indices.refresh(index=INDEX_NAME)

    count = client.count(index=INDEX_NAME)['count']
    print(f"\n✓ Verification: {count} documents in index '{INDEX_NAME}'")

    # Show a sample document
    if count > 0:
        result = client.search(
            index=INDEX_NAME,
            size=1,
            query={"match_all": {}}
        )

        if result['hits']['hits']:
            sample = result['hits']['hits'][0]['_source']
            print(f"\nSample document:")
            print(f"  Filename: {sample.get('filename')}")
            print(f"  File size: {sample.get('file_size')} bytes")
            if 'attachment' in sample:
                att = sample['attachment']
                print(f"  Content type: {att.get('content_type')}")
                print(f"  Content length: {att.get('content_length')} chars")
                print(f"  Language: {att.get('language')}")


def main():
    """Main execution function."""
    print("=" * 60)
    print("PDF to Elasticsearch Upload Script")
    print("=" * 60)
    print()

    try:
        # Step 1: Create Elasticsearch client
        client = create_es_client()
        print()

        # Step 2: Create ingest pipeline
        create_ingest_pipeline(client)
        print()

        # Step 3: Create index with mapping
        create_index_with_mapping(client)
        print()

        # Step 4: Get PDF files
        pdf_files = get_pdf_files(PDF_FOLDER)
        print()

        # Step 5: Upload PDFs
        upload_pdfs(client, pdf_files)
        print()

        # Step 6: Verify upload
        verify_documents(client)
        print()

        print("=" * 60)
        print("✓ All operations completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise


if __name__ == "__main__":
    main()
