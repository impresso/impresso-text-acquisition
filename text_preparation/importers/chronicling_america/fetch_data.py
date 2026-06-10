#!/usr/bin/env python3
"""Downloader utility for fetching raw METS/ALTO files from Chronicling America.

This utility crawls the Library of Congress's open batches directory index
(https://chroniclingamerica.loc.gov/data/batches/) to download issue METS and ALTO files.
It saves them in the folder structure:
  /[input_dir]/[newspaper-name]/[year]/[month]/[day]/[edition]/
"""

import os
import re
import sys
import time
import logging
import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

class ChroniclingAmericaDownloader:
    """Downloader class for fetching raw METS-ALTO data from Chronicling America."""

    def __init__(self, output_dir: str, delay: float = 1.0, max_retries: int = 5):
        """
        Args:
            output_dir (str): Base directory where raw newspaper data should be stored.
            delay (float): Delay in seconds between HTTP requests to avoid rate limits.
            max_retries (int): Maximum number of retries for failed requests.
        """
        self.output_dir = output_dir
        self.delay = delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, method: str = 'GET', **kwargs) -> requests.Response:
        """Helper to make HTTP requests with backoff retry logic for 429 / 503 errors."""
        retries = 0
        backoff = 2.0
        while retries < self.max_retries:
            time.sleep(self.delay)
            try:
                if method == 'GET':
                    response = self.session.get(url, **kwargs)
                else:
                    response = self.session.head(url, **kwargs)

                if response.status_code == 429:
                    logger.warning(f"Got 429 rate limit. Sleeping for {backoff}s and retrying...")
                    time.sleep(backoff)
                    backoff *= 2.0
                    retries += 1
                    continue

                return response
            except requests.RequestException as e:
                logger.warning(f"Request failed: {e}. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2.0
                retries += 1

        raise Exception(f"Failed to fetch URL {url} after {self.max_retries} attempts.")

    def get_batches(self) -> list[str]:
        """Fetch the list of all batches from the main index page."""
        url = "https://chroniclingamerica.loc.gov/data/batches/"
        logger.info(f"Fetching batches from {url}...")
        resp = self._request(url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Extract all directory links ending with '/' and filter
        batches = []
        for a in soup.find_all('a'):
            href = a.get('href', '')
            text = a.text.strip().rstrip('/')
            if text.startswith('dlc_') or text.startswith('dc_') or ('_ver' in text):
                batches.append(text)
        return sorted(list(set(batches)))

    def scan_batch_for_lccn(self, batch: str, lccn: str) -> bool:
        """Check if a batch contains the data directory for the given LCCN."""
        url = f"https://chroniclingamerica.loc.gov/data/batches/{batch}/data/{lccn}/"
        try:
            resp = self._request(url, method='HEAD')
            return resp.status_code == 200
        except Exception:
            return False

    def list_issues_in_batch(self, batch: str, lccn: str) -> list[dict[str, str]]:
        """List all issue directories and their dates within a batch.
        
        Returns:
            list[dict]: A list of issues, each with 'date' (YYYY-MM-DD), 'edition', and 'url'.
        """
        lccn_url = f"https://chroniclingamerica.loc.gov/data/batches/{batch}/data/{lccn}/"
        logger.info(f"Listing reels for LCCN {lccn} in batch {batch}...")
        resp = self._request(lccn_url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        reels = []
        for a in soup.find_all('a'):
            href = a.get('href', '')
            text = a.text.strip().rstrip('/')
            # Reel directories are typically numeric/alphanumeric
            if text and not text.startswith('.') and text != '../':
                reels.append(text)
                
        issues = []
        for reel in reels:
            reel_url = f"{lccn_url}{reel}/"
            logger.info(f"Scanning reel {reel}...")
            resp = self._request(reel_url)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            for a in soup.find_all('a'):
                text = a.text.strip().rstrip('/')
                # Issue folders are named YYYYMMDDxx where xx is the edition number
                if re.match(r'^\d{10}$', text):
                    date_str = f"{text[:4]}-{text[4:6]}-{text[6:8]}"
                    edition_str = f"ed-{int(text[8:10])}"
                    issues.append({
                        'date': date_str,
                        'edition': edition_str,
                        'issue_dir_name': text,
                        'url': f"{reel_url}{text}/"
                    })
        return issues

    def download_issue(self, issue_info: dict[str, str], alias: str) -> str:
        """Download all METS and ALTO files for a given issue into the local workspace.
        
        Returns:
            str: The local path to the downloaded issue.
        """
        issue_url = issue_info['url']
        dir_name = issue_info['issue_dir_name']
        date_parts = issue_info['date'].split('-')
        year, month, day = date_parts[0], date_parts[1], date_parts[2]
        edition = issue_info['edition']

        # Format: [output_dir]/[newspaper-name]/[year]/[month]/[day]/[edition]/
        local_issue_dir = os.path.join(self.output_dir, alias, year, month, day, edition)
        local_alto_dir = os.path.join(local_issue_dir, 'alto')
        
        os.makedirs(local_issue_dir, exist_ok=True)
        os.makedirs(local_alto_dir, exist_ok=True)
        
        logger.info(f"Downloading issue from {issue_url} to {local_issue_dir}...")
        resp = self._request(issue_url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        files_to_download = []
        for a in soup.find_all('a'):
            href = a.get('href', '')
            text = a.text.strip()
            if text.endswith('.xml'):
                files_to_download.append(text)
                
        # METS file is named [issue_dir_name].xml (e.g. 1916030101.xml)
        # ALTO files are usually other XMLs
        for fname in files_to_download:
            file_url = f"{issue_url}{fname}"
            if fname == f"{dir_name}.xml" or 'mets' in fname.lower():
                # Store METS in the main issue folder
                dest_path = os.path.join(local_issue_dir, fname)
            else:
                # Store ALTO XMLs in the alto/ subfolder
                dest_path = os.path.join(local_alto_dir, fname)
                
            logger.info(f"Downloading {fname}...")
            f_resp = self._request(file_url)
            with open(dest_path, 'wb') as f:
                f.write(f_resp.content)
                
        return local_issue_dir

def main():
    parser = argparse.ArgumentParser(description="Download METS/ALTO files for a specific LCCN.")
    parser.add_argument('--lccn', type=str, default='sn83045462', help="LCCN of the newspaper (default: sn83045462 for Evening Star)")
    parser.add_argument('--alias', type=str, default='eveningstar', help="Alias name for directory (default: eveningstar)")
    parser.add_argument('--output-dir', type=str, required=True, help="Directory to save downloaded files")
    parser.add_argument('--date', type=str, help="Specific date to download (YYYY-MM-DD), if omitted downloads a sample issue")
    parser.add_argument('--limit', type=int, default=1, help="Max issues to download")
    parser.add_argument('--batch', type=str, help="Specify direct batch name (e.g. dlc_1arp_ver01) to bypass searching batches")
    
    args = parser.parse_args()
    
    downloader = ChroniclingAmericaDownloader(args.output_dir)
    
    if args.batch:
        batch = args.batch
        logger.info(f"Using specified batch: {batch}")
    else:
        batches = downloader.get_batches()
        logger.info(f"Found {len(batches)} total batches. Searching for batches containing {args.lccn}...")
        
        matching_batches = []
        # Search for LCCN in batches (prioritize dlc_ ones)
        for batch in batches:
            if downloader.scan_batch_for_lccn(batch, args.lccn):
                logger.info(f"Found matching batch: {batch}")
                matching_batches.append(batch)
                break # just need one batch for sample
                
        if not matching_batches:
            logger.error(f"Could not find any batch containing LCCN {args.lccn}")
            sys.exit(1)
            
        batch = matching_batches[0]
    issues = downloader.list_issues_in_batch(batch, args.lccn)
    logger.info(f"Found {len(issues)} issues for LCCN {args.lccn} in batch {batch}")
    
    if args.date:
        filtered_issues = [i for i in issues if i['date'] == args.date]
    else:
        filtered_issues = issues[:args.limit]
        
    if not filtered_issues:
        logger.error(f"No issues found matching parameters.")
        sys.exit(1)
        
    for issue in filtered_issues:
        downloader.download_issue(issue, args.alias)
        
    logger.info("Download completed successfully!")

if __name__ == '__main__':
    main()
