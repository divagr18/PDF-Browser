
from django import forms

class PdfUploadForm(forms.Form):
    pdf_file = forms.FileField(
        label='Select a PDF file',
        help_text='Only PDF files are allowed.',
        widget=forms.ClearableFileInput(attrs={'accept': '.pdf'}) 
    ) 
    def clean_pdf_file(self):
        file = self.cleaned_data.get('pdf_file')
        if file:
            if not file.name.lower().endswith('.pdf'):
                raise forms.ValidationError("Only PDF files are allowed.")
        return file