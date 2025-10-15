import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import time
from pathlib import Path

class PDFCrawler:
    def __init__(self, base_url, output_dir="downloaded_pdfs"):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.output_dir = output_dir
        self.visited_urls = set()
        self.downloaded_pdfs = set()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Create output directory
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
    
    def is_valid_url(self, url):
        """Check if URL belongs to the same domain"""
        parsed = urlparse(url)
        return parsed.netloc == self.domain or parsed.netloc == ''
    
    def get_pdf_filename(self, url):
        """Extract filename from URL"""
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        if not filename.endswith('.pdf'):
            filename += '.pdf'
        return filename
    
    def download_pdf(self, pdf_url):
        """Download a PDF file"""
        if pdf_url in self.downloaded_pdfs:
            return
        
        try:
            print(f"Downloading: {pdf_url}")
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            filename = self.get_pdf_filename(pdf_url)
            filepath = os.path.join(self.output_dir, filename)
            
            # Handle duplicate filenames
            counter = 1
            base_name, ext = os.path.splitext(filename)
            while os.path.exists(filepath):
                filename = f"{base_name}_{counter}{ext}"
                filepath = os.path.join(self.output_dir, filename)
                counter += 1
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            self.downloaded_pdfs.add(pdf_url)
            print(f"✓ Saved: {filename}")
            
        except Exception as e:
            print(f"✗ Error downloading {pdf_url}: {str(e)}")
    
    def crawl_page(self, url, max_depth=3, current_depth=0):
        """Recursively crawl pages and find PDFs"""
        if current_depth > max_depth or url in self.visited_urls:
            return
        
        if not self.is_valid_url(url):
            return
        
        self.visited_urls.add(url)
        print(f"\nCrawling [{current_depth}]: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Check if the current URL is a PDF
            if response.headers.get('Content-Type', '').startswith('application/pdf'):
                self.download_pdf(url)
                return
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all links
            for link in soup.find_all(['a', 'link'], href=True):
                href = link['href']
                full_url = urljoin(url, href)
                
                # Download if it's a PDF
                if href.lower().endswith('.pdf') or '.pdf' in href.lower():
                    self.download_pdf(full_url)
                # Otherwise, crawl the page
                elif self.is_valid_url(full_url) and full_url not in self.visited_urls:
                    time.sleep(0.5)  # Be polite, avoid overwhelming the server
                    self.crawl_page(full_url, max_depth, current_depth + 1)
        
        except Exception as e:
            print(f"✗ Error crawling {url}: {str(e)}")
    
    def start(self, max_depth=3):
        """Start the crawling process"""
        print(f"Starting PDF crawler for: {self.base_url}")
        print(f"Output directory: {self.output_dir}")
        print(f"Max depth: {max_depth}\n")
        
        self.crawl_page(self.base_url, max_depth)
        
        print(f"\n{'='*60}")
        print(f"Crawling complete!")
        print(f"Total pages visited: {len(self.visited_urls)}")
        print(f"Total PDFs downloaded: {len(self.downloaded_pdfs)}")
        print(f"Files saved in: {os.path.abspath(self.output_dir)}")
        print(f"{'='*60}")

if __name__ == "__main__":
    # Configuration
    BASE_URL = "https://www.greatamericaninsurancegroup.com/"
    OUTPUT_DIR = "great_american_pdfs"
    MAX_DEPTH = 3  # Adjust based on website structure
    
    # Create and run crawler
    crawler = PDFCrawler(BASE_URL, OUTPUT_DIR)
    crawler.start(max_depth=MAX_DEPTH)
