import requests
import json
from pypdf import PdfReader
import os
import re

# These URLs are borrowed from the OpenStates project 
# (https://github.com/openstates/openstates-scrapers/blob/main/scrapers/me/events.py)
TESTIMONY_BASE_URI = (
    "http://legislature.maine.gov/backend/"
    "breeze/data/CommitteeTestimony?"
    "$filter=(Request%2FLDNumber%20eq%20{})%20and"
    "%20(Request%2FLegislature%20eq%20{})"
    "&$orderby=LastName%2CFirstName%2COrganization&"
    "$expand=Request&$select=Id%2CFileType%2CNamePrefix"
    "%2CFirstName%2CLastName%2COrganization%2CPresentedDate%2CFileSize%2CTopic"
)
DOCUMENT_BASE_URI = (
    "http://legislature.maine.gov/backend/app/services"
    "/getDocument.aspx?doctype=test&documentId={}"
)
LD_BASE_URI = (
    "http://lldc.mainelegislature.org/Open/LDs/{}/"
)

def sanitize_filename(filename):
    return re.sub(r'[^\w\-_\. \(\)]', '_', filename)

class BillScraper():
    def __init__(self, session=131, output_dir="data/131/bills/"):
        self.s = requests.Session()
        self.ld_base_url = LD_BASE_URI.format(session)
        self.output_dir = output_dir
    
    def get_testimony(self, ldno):
        url = TESTIMONY_BASE_URI.format(ldno, "131")
        res = self.s.get(url)
        testimony = json.loads(res.text)
        return testimony

    def download_bill(self, ldtxt, output_dir=None):
        if not output_dir:
            output_dir = self.output_dir
        fname = sanitize_filename(ldtxt + ".pdf")
        res = self.s.get(self.ld_base_url + fname, timeout=10)
        filepath = os.path.join(output_dir, fname)
        with open(os.path.join(output_dir, fname), 'wb') as f:
            f.write(res.content)
        return filepath

    def download_testimony(self, document_number, document_name, output_dir=None):
        if not output_dir:
            output_dir = self.output_dir
        fname = sanitize_filename(document_name)
        if not fname.endswith(".pdf"):
            fname += ".pdf"
        res = self.s.get(DOCUMENT_BASE_URI.format(document_number), timeout=10)
        filepath = os.path.join(output_dir, fname)
        with open(filepath, "wb") as f:
            f.write(res.content)
        return filepath

    def pdf_to_txt(self, filepath, output_dir=None):
        if not output_dir:
            output_dir = self.output_dir
        
        # Attempt to read filepath as a filename in output_dir
        # if it is not a valid file
        if not os.path.isfile(filepath):
            if not filepath.endswith(".pdf"):
                filepath += ".pdf"
            filepath = os.path.join(output_dir, filepath)
        
        try:
            reader = PdfReader(filepath)
        except:
            print("Error: could not read file {}".format(filepath))
            return None
        
        lines = []
        for page in reader.pages:
            # Extract text from the whole page
            text_all = page.extract_text()
            # Split text at line breaks
            lines.extend(text_all.split('\n'))

        # Write all text to .txt
        with open(filepath.replace(".pdf", ".txt"), "w") as file:
            file.writelines([line + "\n" for line in lines])
        
        return filepath

