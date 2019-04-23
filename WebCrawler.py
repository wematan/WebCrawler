import os
import sys
import argparse
import math
import re
import hashlib
md5 = hashlib.md5

from urllib.parse import urlparse
from functools import partial

from CrawlerWorker import CrawlerWorker
from CrawlerReporter import CrawlerReporter

import multiprocessing as mp
POOL_SIZE = mp.cpu_count()
global CRAWLER_COUNTER
CRAWLER_COUNTER = mp.Value('i', 0)


class WebCrawler:

    def __init__(self, base_url, max_depth=math.inf):
        self.base_url = base_url
        self.link_data = {}
        self.image_data = {}
        self.pool_map = mp.Pool(POOL_SIZE).map_async
        self.max_depth = max_depth

    def start(self):
        self.crawl([self.base_url])

    def crawl(self, urls, depth=0):
        if depth > self.max_depth or CRAWLER_COUNTER.value > 100:
            return
        sys.stdout.write("\rScraped %d Links So Far." % (CRAWLER_COUNTER.value))
        sys.stdout.flush()
        inbound, outbound = self.filter_inbound_urls(urls)
        inboud_results = self.pool_map(CrawlerWorker.get_links, inbound)
        outbound_results = self.pool_map(partial(CrawlerWorker.get_links, scan_images=False), (outbound))
        for res in outbound_results.get():
            url, links, status, images = res
            self.register_results(url, depth, status)
        for res in inboud_results.get():
            url, links, status, images = res
            self.register_results(url, depth, status)
            self.find_duplicate_images(images)
            self.crawl(links, depth+1)
        return

    def filter_inbound_urls(self, urls):
        inbound = []
        outbound = []
        for url in urls:
            if url.split("://")[-1] not in map(lambda u: u.split('://')[-1], self.link_data.keys()):
                if self.base_url == url or urlparse(self.base_url).geturl() == urlparse(url).hostname:
                    inbound.append(url)
                else:
                    outbound.append(url)
        return set(inbound), set(outbound)

    def register_results(self, url, depth, status):
        self.link_data[url] = {
            'url': url,
            'depth': depth,
            'status': status}
        with CRAWLER_COUNTER.get_lock():
            CRAWLER_COUNTER.value = len(self.link_data.keys())

    def find_duplicate_images(self, images):
        for img, digest in images.items():
            if digest not in self.image_data.values():
                self.image_data[img] = digest
            else:
                for im, dig in self.image_data.items():
                    if dig == digest and self.get_clean_url(im) != self.get_clean_url(img):
                        print("\nDuplicate images - %s == %s" % (img, im))
                        break
        return

    def get_clean_url(self, url):
        ret_val = url.split("://")[-1]
        ret_val = re.sub("/{2,}", "/", ret_val)
        return ret_val

    def data(self):
        return self.link_data



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Multiprocess Web Crawler")
    parser.add_argument('-i', '--input-url', type=str, default='www.guardicore.com')
    parser.add_argument('-o', '--output-report', type=str, default=os.path.join(os.path.abspath(os.path.curdir), "CrawlerReport"))
    args = parser.parse_args()

    wc = WebCrawler(args.input_url)
    wc.start()
    cr = CrawlerReporter(wc.data(), args.output_report)
    cr.write_report()
