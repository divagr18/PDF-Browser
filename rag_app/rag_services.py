import os
import re
from django.conf import settings
from langchain_community.document_loaders import PyPDFLoader 
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from langchain_openai import ChatOpenAI
from langchain.chains import RetrievalQA
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from typing import Generator, Dict, Any
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationalRetrievalChain
def process_pdf_and_store(pdf_path: str, collection_name: str) -> bool:
    """
    Processes a PDF file: extracts text, chunks it, generates embeddings,
    and stores them in a ChromaDB collection.

    Args:
        pdf_path: The full path to the PDF file.
        collection_name: The name for the ChromaDB collection.

    Returns:
        True if processing was successful, False otherwise.
    """
    print(f"--- Starting processing for: {pdf_path} ---")
    print(f"--- Using collection name: {collection_name} ---")
    print(f"--- Storing in: {settings.VECTOR_STORE_DIR} ---")

    try:
        
        print("Step 1: Loading PDF...")
        loader = PyPDFLoader(pdf_path)
        
        documents = loader.load()
        if not documents:
            print("Error: No content extracted from PDF.")
            return False
        print(f"Loaded {len(documents)} document pages.")

        
        full_text = "\n".join([doc.page_content for doc in documents])
        if not full_text.strip():
            print("Error: Extracted text is empty.")
            return False
        print(f"Extracted ~{len(full_text)} characters.")


        
        print("Step 2: Chunking text...")
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,  
            chunk_overlap=200   
        )
        
        text_chunks = text_splitter.split_text(full_text)

        
        
        
        chunked_documents = text_splitter.create_documents(text_chunks)

        if not chunked_documents:
            print("Error: Text splitting resulted in no chunks.")
            return False
        print(f"Split into {len(chunked_documents)} chunks.")


        
        print("Step 3: Initializing OpenAI Embeddings...")
        
        
        embeddings = OpenAIEmbeddings(
             model="text-embedding-3-small" 
             
        )


        
        print("Step 4: Creating/Updating Vector Store (ChromaDB)...")
        
        
        vector_store = Chroma.from_documents(
            documents=chunked_documents,
            embedding=embeddings,
            collection_name=collection_name,
            persist_directory=settings.VECTOR_STORE_DIR
        )

        
        vector_store.persist()
        print(f"--- Successfully processed and stored embeddings in collection '{collection_name}' ---")
        return True

    except Exception as e:
        print(f"Error during PDF processing: {e}")
        import traceback
        traceback.print_exc() 
        return False

def generate_collection_name(pdf_filename: str) -> str:
    """
    Generates a safe collection name from a PDF filename.
    Removes extension, replaces non-alphanumeric with underscores.
    """
    name_without_ext = os.path.splitext(pdf_filename)[0]
    
    safe_name = re.sub(r'[^\w-]', '_', name_without_ext)
    
    safe_name = re.sub(r'_+', '_', safe_name).strip('_')
    
    
    
    
    

    
    if len(safe_name) < 3:
        safe_name = safe_name + "___" 
    safe_name = safe_name[:63] 

    
    if not safe_name[0].isalnum():
        safe_name = "c" + safe_name[1:]
    if not safe_name[-1].isalnum():
         safe_name = safe_name[:-1] + "e"

    
    safe_name = re.sub(r'_+', '_', safe_name)

    
    if not safe_name:
        import uuid
        return f"pdf_{uuid.uuid4().hex[:8]}" 

    print(f"Generated collection name: {safe_name} for filename: {pdf_filename}")
    return safe_name.lower() 
 
def query_rag_pipeline_stream(collection_name: str, query: str) -> Generator[str, None, None]:
    """
    Queries the RAG pipeline and streams the LLM response chunks.

    Args:
        collection_name: The name of the ChromaDB collection to query.
        query: The user's question.

    Yields:
        Chunks of the generated answer string.

    Raises:
        ValueError: If the vector store cannot be loaded or is empty.
        Exception: For other underlying errors during processing.
    """
    print(f"--- Streaming query for collection '{collection_name}' ---")
    try:
        
        print("Step 1: Initializing OpenAI Embeddings for query...")
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

        
        print("Step 2: Loading Vector Store...")
        vector_store = Chroma(
            collection_name=collection_name,
            embedding_function=embeddings,
            persist_directory=settings.VECTOR_STORE_DIR
        )
        
        try:
            vector_store.similarity_search("test", k=1)
            print("Vector store loaded successfully.")
        except Exception as e:
            print(f"Error: Vector store '{collection_name}' seems empty or corrupted: {e}")
            
            raise ValueError(f"Data for this PDF (collection: {collection_name}) seems corrupted or missing.")

        
        print("Step 3: Initializing ChatOpenAI LLM...")
        llm = ChatOpenAI(
            model_name="gpt-4o-mini", 
            temperature=0.7,
            streaming=True 
        )

        
        print("Step 4: Creating retriever...")
        retriever = vector_store.as_retriever(
            search_type="similarity",
            search_kwargs={'k': 5} 
        )

        
        print("Step 5: Creating RAG chain...")
        template = """Answer the question based only on the following context, do not mention the context itself when answering:
        {context}

        Question: {question}

        Answer:""" 
        prompt = PromptTemplate.from_template(template)

        rag_chain = (
            {"context": retriever, "question": RunnablePassthrough()}
            | prompt
            | llm
            | StrOutputParser()
        )

        
        print("Step 6: Streaming RAG chain response...")
        
        for chunk in rag_chain.stream(query):
            
            yield chunk 

        print(f"--- Streaming finished for collection '{collection_name}' ---")

    except ValueError as ve: 
         print(f"ValueError during RAG stream: {ve}")
         raise ve 
    except Exception as e:
        print(f"Error during RAG stream: {e}")
        import traceback
        traceback.print_exc()
        
        raise Exception(f"An error occurred while generating the answer stream: {e}")