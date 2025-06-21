from django.shortcuts import render, redirect
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.http import JsonResponse
import os
import json
from .forms import PdfUploadForm
from .tasks import process_pdf_task
from .rag_services import query_rag_pipeline_stream, generate_collection_name
from celery.result import AsyncResult
from django.http import JsonResponse, StreamingHttpResponse


def upload_pdf_view(request):
    """Handles PDF file uploads and monitors asynchronous processing status.

    Args:
        request (HttpRequest): The HTTP request object containing form data, files, and session.

    Returns:
        HttpResponse: Renders the PDF upload page with form, status messages, and processing state or redirects after upload/clear actions.

    This view manages:
    - Displaying the upload form.
    - Handling PDF file uploads and triggering background processing via a Celery task.
    - Checking the status of the background task and updating session data accordingly.
    - Clearing session data on user request.
    - Providing user feedback messages related to upload and processing states."""
    message = None
    message_type = None
    form = PdfUploadForm()
    task_id = request.session.get("processing_task_id")
    processed_collection_name = request.session.get("processed_collection_name")
    uploaded_pdf_name = request.session.get("uploaded_pdf_name")
    if task_id and not processed_collection_name:
        task_result = AsyncResult(task_id)
        if task_result.ready():
            if task_result.successful():
                result_data = task_result.result
                processed_collection_name = result_data.get("collection_name")
                uploaded_pdf_name = result_data.get("original_filename")
                request.session["processed_collection_name"] = processed_collection_name
                request.session["uploaded_pdf_name"] = uploaded_pdf_name
                message = f"Successfully processed '{uploaded_pdf_name}'. Ready for questions."
                message_type = "success"
            else:
                error_message = (
                    str(task_result.traceback)
                    if task_result.traceback
                    else "Unknown processing error."
                )
                message = f"Failed to process the PDF. Error: {error_message}"
                message_type = "error"
            request.session.pop("processing_task_id", None)
            task_id = None
        else:
            message = "Your PDF is still processing in the background. Please wait..."
            message_type = "info"
    elif processed_collection_name:
        message = f"Ready to answer questions about: {uploaded_pdf_name}"
        message_type = "info"
    if request.method == "POST":
        if "clear" in request.POST:
            request.session.pop("processed_collection_name", None)
            request.session.pop("uploaded_pdf_name", None)
            request.session.pop("processing_task_id", None)
            print("Session cleared.")
            return redirect("upload_pdf")
        form = PdfUploadForm(request.POST, request.FILES)
        if form.is_valid():
            pdf_file = request.FILES["pdf_file"]
            fs = FileSystemStorage(location=settings.MEDIA_ROOT)
            filename = fs.save(pdf_file.name, pdf_file)
            uploaded_file_path = fs.path(filename)
            print(f"Dispatching process_pdf_task for {uploaded_file_path}")
            task = process_pdf_task.delay(uploaded_file_path, filename)
            request.session["processing_task_id"] = task.id
            request.session["uploaded_pdf_name"] = filename
            request.session.pop("processed_collection_name", None)
            message = f"'{filename}' uploaded. Processing started in the background (Task ID: {task.id}). Refresh page for status."
            message_type = "info"
            return redirect("upload_pdf")
        else:
            message = "Upload failed. Please check the errors below."
            message_type = "error"
            request.session.pop("processed_collection_name", None)
            request.session.pop("uploaded_pdf_name", None)
            request.session.pop("processing_task_id", None)
    return render(
        request,
        "rag_app/upload_pdf.html",
        {
            "form": form,
            "message": message,
            "message_type": message_type,
            "uploaded_pdf_name": uploaded_pdf_name,
            "processed_collection_name": processed_collection_name,
            "is_processing": task_id is not None and not processed_collection_name,
        },
    )


def get_task_status(request, task_id):
    """Returns a JSON response with the current status and details of a Celery task.

    Args:
        request (HttpRequest): The Django HTTP request object.
        task_id (str): The unique identifier of the Celery task to query.

    Returns:
        JsonResponse: A JSON object containing the task ID, status, result or progress information,
        any error information if the task failed, and relevant task step details.

    Updates the user's session with processed collection and file info upon successful task completion,
    and clears the processing task ID when the task finishes or fails."""
    task_result = AsyncResult(task_id)
    response_data = {
        "task_id": task_id,
        "status": task_result.status,
        "result": None,
        "error": None,
        "step": None,
        "step_status": None,
    }
    if task_result.status == "PENDING":
        response_data["result"] = "Task is waiting to be processed."
    elif task_result.status == "STARTED":
        response_data["result"] = "Task has started."
    elif task_result.status == "PROGRESS":
        response_data["result"] = task_result.info.get("status", "Processing...")
        response_data["step"] = task_result.info.get("step")
        response_data["step_status"] = task_result.info.get("status")
    elif task_result.status == "SUCCESS":
        result_data = task_result.result
        response_data["result"] = result_data
        request.session["processed_collection_name"] = result_data.get(
            "collection_name"
        )
        request.session["uploaded_pdf_name"] = result_data.get("original_filename")
        request.session.pop("processing_task_id", None)
    elif task_result.status == "FAILURE":
        response_data["error"] = (
            str(task_result.traceback)
            if task_result.traceback
            else "Unknown processing error."
        )
        request.session.pop("processing_task_id", None)
    return JsonResponse(response_data)


def clear_session_view(request):
    """Clears specific session variables related to PDF processing and redirects to the upload page.

    This view removes the 'processed_collection_name', 'uploaded_pdf_name', and 'processing_task_id' keys
    from the user's session if they exist, effectively resetting the state for a new upload or process.

    Args:
        request (HttpRequest): The incoming HTTP request object containing session data.

    Returns:
        HttpResponseRedirect: A redirect response to the 'upload_pdf' route for starting a new upload."""
    request.session.pop("processed_collection_name", None)
    request.session.pop("uploaded_pdf_name", None)
    request.session.pop("processing_task_id", None)
    print("Session cleared.")
    return redirect("upload_pdf")


def query_view(request):
    """Handles POST requests to perform a query against a previously processed PDF collection and streams the response.

    Args:
        request (HttpRequest): The incoming HTTP request, expected to be a POST containing JSON with a 'query' field.

    Returns:
        StreamingHttpResponse: A streaming response that yields chunks of data from the query pipeline if successful.
        JsonResponse: An error response with an appropriate HTTP status code for:
            - Invalid request method (405)
            - Missing or expired PDF processing session (400)
            - Missing query in request body (400)
            - Malformed JSON in request body (400)
            - Unexpected server errors before streaming (500)

    Notes:
        The function relies on `request.session['processed_collection_name']` to identify the collection to query.
        It uses `query_rag_pipeline_stream` to generate streaming query results.
        Errors encountered during streaming are caught and yielded as stream error messages prefixed with "STREAM_ERROR:"."""
    if request.method != "POST":
        return JsonResponse({"error": "Invalid request method"}, status=405)
    collection_name = request.session.get("processed_collection_name")
    if not collection_name:
        return JsonResponse(
            {"error": "No PDF processed or session expired."}, status=400
        )
    try:
        data = json.loads(request.body)
        user_query = data.get("query")
        if not user_query:
            return JsonResponse({"error": "No query provided"}, status=400)

        def stream_response_generator():
            try:
                for chunk in query_rag_pipeline_stream(collection_name, user_query):
                    yield chunk
            except ValueError as ve:
                print(f"Caught ValueError in stream generator: {ve}")
                yield f"STREAM_ERROR: ValueError: {ve}"
            except Exception as e:
                print(f"Caught Exception in stream generator: {e}")
                yield f"STREAM_ERROR: Exception: An unexpected error occurred during streaming."

        response = StreamingHttpResponse(
            stream_response_generator(), content_type="text/plain"
        )
        return response
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON format"}, status=400)
    except Exception as e:
        print(f"Unexpected error in query_view before streaming: {e}")
        return JsonResponse(
            {"error": "An unexpected server error occurred before streaming."},
            status=500,
        )
