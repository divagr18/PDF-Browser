# rag_app/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('', views.upload_pdf_view, name='upload_pdf'),
    path('clear/', views.clear_session_view, name='clear_session'),
    path('query/', views.query_view, name='query'), # Add this line
    path('task_status/<str:task_id>/', views.get_task_status, name='get_task_status'),

]