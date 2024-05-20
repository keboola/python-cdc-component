# Convert AsciiDoc to Markdown using Asciidoctor and markdownify
import subprocess

import markdownify
import requests
from bs4 import BeautifulSoup

# Convert AsciiDoc to HTML using Asciidoctor
url = 'https://debezium.io/documentation/reference/stable/connectors/mysql.html'
response = requests.get(url)
html_content = response.text

# Convert HTML to Markdown
soup = BeautifulSoup(html_content, 'html.parser')
md_content = markdownify.markdownify(str(soup), heading_style="ATX")

# Save Markdown to a file
md_file_path = 'mysql_docs_converted.md'
with open(md_file_path, 'w') as f:
    f.write(md_content)
