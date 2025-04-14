import os
import re
from celery import shared_task
from django.conf import settings
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma


from .rag_services import generate_collection_name

@shared_task(bind=True) 
def process_pdf_task(self, pdf_path: str, original_filename: str):
    """
    Celery task to process a PDF file asynchronously.
    Extracts text, chunks, embeds, and stores in ChromaDB.

    Args:
        pdf_path: Full path to the uploaded PDF file.
        original_filename: The original name of the uploaded file.

    Returns:
        A dictionary with status and collection name or error message.
    """
    task_id = self.request.id
    print(f"--- [Task {task_id}] Starting processing for: {pdf_path} ---")

    collection_name = generate_collection_name(original_filename)
    print(f"--- [Task {task_id}] Using collection name: {collection_name} ---")
    print(f"--- [Task {task_id}] Storing in: {settings.VECTOR_STORE_DIR} ---")

    try:
        
        print(f"[Task {task_id}] Step 1: Loading PDF...")
        self.update_state(state='PROGRESS', meta={'step': 1, 'status': 'Loading PDF...'})
        loader = PyPDFLoader(pdf_path)
        documents = loader.load()
        if not documents:
            raise ValueError("No content extracted from PDF.")
        print(f"[Task {task_id}] Loaded {len(documents)} document pages.")
        full_text = "\n".join([doc.page_content for doc in documents])
        if not full_text.strip():
            raise ValueError("Extracted text is empty.")
        print(f"[Task {task_id}] Extracted ~{len(full_text)} characters.")
        print(f"[Task {task_id}] Step 2: Chunking text...")
        self.update_state(state='PROGRESS', meta={'step': 2, 'status': 'Chunking text...'})
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        chunked_documents = text_splitter.create_documents(text_splitter.split_text(full_text))
        if not chunked_documents:
            raise ValueError("Text splitting resulted in no chunks.")
        print(f"[Task {task_id}] Split into {len(chunked_documents)} chunks.")
        print(f"[Task {task_id}] Step 3: Initializing OpenAI Embeddings...")
        self.update_state(state='PROGRESS', meta={'step': 3, 'status': 'Initializing embeddings...'})
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        print(f"[Task {task_id}] Step 4: Creating/Updating Vector Store...")
        self.update_state(state='PROGRESS', meta={'step': 4, 'status': 'Storing embeddings...'})
        vector_store = Chroma.from_documents(
            documents=chunked_documents,
            embedding=embeddings,
            collection_name=collection_name,
            persist_directory=settings.VECTOR_STORE_DIR
        )
        vector_store.persist() 

        print(f"--- [Task {task_id}] Successfully processed and stored embeddings ---")
        return {'status': 'SUCCESS', 'collection_name': collection_name, 'original_filename': original_filename}

    except Exception as e:
        print(f"!!! [Task {task_id}] Error during PDF processing: {e} !!!")
        import traceback
        traceback.print_exc()    
        self.update_state(state='FAILURE', meta={'exc_type': type(e).__name__, 'exc_message': str(e)})   
        return {'status': 'FAILURE', 'error': str(e)}