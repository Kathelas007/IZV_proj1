import os
import pathlib
import requests
from bs4 import BeautifulSoup
import zipfile

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.11; rv:83.0) Gecko/20100101 Firefox/83.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}


class DataDownloader():
    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="ataf", cache_filename="ata_{}.pkl.gz"):
        """
        :param url: from which URL will be data downloaded
        :param folder: folder for tmp data
        :param cache_filename: file name in 'folder' with data processed with get_list
        """

        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename

    def iter_file_names(self, html_response):
        """
        Fnc finds html table element and iterates throught its zip file names
        :return: current file name
        """

        soup = BeautifulSoup(html_response.text, 'html.parser')
        table_el = soup.find("table", {'class': 'table table-fluid'})
        for tr_el in table_el.find_all('a', {'class': ['btn', 'btm-sm', 'btm-primary']}):
            if tr_el['href'].endswith('zip'):
                yield tr_el['href']

    def correct_irregular_file_names(self, file_name: str) -> str:
        if file_name.startswith('data-gis'):
            return file_name.replace('data-gis', 'datagis')

        if file_name == 'datagis2016.zip':
            return 'datagis-12-2016.zip'

        if 'rok' in file_name:
            return file_name.replace('rok', '12')

        pattern = 'datagis-'
        if not file_name.startswith(pattern):
            index = len(pattern) - 1
            return file_name[:index] + '-' + file_name[index:]

        return file_name

    def download_file(self, file_name: str):
        """
        Fnc downloads 1 file if is not already downloaded
        """
        zip_file = os.path.split(file_name)[-1]
        zip_file = self.correct_irregular_file_names(zip_file)
        zip_file = os.path.join(self.folder, zip_file)
        full_url = os.path.join(self.url, file_name)

        if os.path.isfile(zip_file): return

        with requests.get(full_url, stream=True) as r:
            with open(zip_file, 'wb') as f:
                for chunk in r:
                    f.write(chunk)

    def download_data(self):
        """
        Fnc downloads all files from 'URL' to 'folder'
        """
        pathlib.Path(self.folder).mkdir(parents=True, exist_ok=True)

        with requests.Session() as s:
            html_response = s.get(self.url, headers=headers)
            for file_names in self.iter_file_names(html_response):
                self.download_file(file_names)

    def parse_region_data(self, region):
        """
        Fnc parses specified 'region' of downloaded data . If data are not downloaded, it downloads them.
        :param region: region code (3 chars)
        :return: ??

        ...

    def get_list(self, regions=None):
        """
        Fnc returns data parsed by parse_region_data
        :param regions: list of selected regions, None means all regions
        :return:
        """
        ...


if __name__ == '__main__':
    d = DataDownloader()
    d.download_data()
    # print(d.correct_irregular_file_names('datagis-01-2018.zip'))
