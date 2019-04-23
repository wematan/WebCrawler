import requests
from urllib.parse import urlparse, urldefrag
from bs4 import BeautifulSoup
import hashlib

class CrawlerWorker:
    md5 = hashlib.md5
    @staticmethod
    def uri_validator(x):
        try:
            result = urlparse(x)
            return all([result.scheme, result.netloc, result.path])
        except:
            return False

    @staticmethod
    def get_links(url, scan_images=True):
        images = {}
        link_list = set()
        headers = {'Accept-Encoding': 'identity'}
        url = "http://" + url.split("://")[-1]
        try:
            res = requests.get(url, headers=headers, timeout=10)
        except requests.exceptions.ConnectionError or requests.exceptions.Timeout:
            return url, [], "Connection Refused" ,images
        if res.status_code != 200:
            return url, [], res.status_code ,images
        soup = BeautifulSoup(res.content, "lxml")

        # Find all a tags
        for link in soup.find_all('a', href=True):
            # Ignore mailing links
            if link.has_attr("href") and "mailto:" not in link['href']:
                if CrawlerWorker.uri_validator(link["href"]):
                    temp_url = link['href'].strip()
                else:
                    temp_url = "/".join([url, link["href"]]).strip()
                # Remove Fragments (Same Link)
                temp_url = urldefrag(temp_url)[0]
                link_list.add(temp_url)

        if scan_images:
            images = CrawlerWorker.extract_images(url, soup)

        return url, list(link_list), res.status_code, images

    @staticmethod
    def extract_images(url, soup):
        image_data = {}
        for img in soup.find_all('img'):
            img_url = img.get('src')
            if img_url:
                if CrawlerWorker.uri_validator(img_url):
                    img_url = img_url.strip()
                else:
                    img_url = "/".join([url, img_url]).strip()
                img_obj = requests.get(img_url, stream=True)
                if img_obj.status_code == 200:
                    img_digest = CrawlerWorker.md5(img_obj.raw.read()).hexdigest()
                    image_data[img_url] = img_digest
        return image_data