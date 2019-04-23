import csv


class CrawlerReporter:
    def __init__(self, data, output_report_name):
        self.data = data

        self.links_report_file = open(output_report_name + "_links.csv", 'w', newline='')
        self.broken_links_report_file = open(output_report_name + "_broken_links.csv", 'w', newline='')

        self.headers = ['url', 'depth', 'status']
        self.links_report = csv.DictWriter(self.links_report_file, fieldnames=self.headers)
        self.links_report.writeheader()

        self.broken_links_report = csv.DictWriter(self.broken_links_report_file, fieldnames=self.headers)
        self.broken_links_report.writeheader()


    def write_report(self):
        for i in self.data:
            if self.data[i]['status'] == 200:
                self.links_report.writerow(self.data[i])
            else:
                self.broken_links_report.writerow(self.data[i])