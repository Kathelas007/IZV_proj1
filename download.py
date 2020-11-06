import os
from os import path
import pathlib
import requests
from bs4 import BeautifulSoup
import zipfile
import logging
import csv
from io import TextIOWrapper

regions = {
    'PHA': '00',
    'STC': '01',
    'JHC': '02',
    'PLK': '03',
    'ULK': '04',
    'HKK': '05',
    'JHM': '06',
    'MSK': '07',
    'OLK': '14',
    'ZLK': '15',
    'VYS': '16',
    'PAK': '17',
    'LBK': '18',
    'KVK': '19',
}


class DataDownloader():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.11; rv:83.0) Gecko/20100101 Firefox/83.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    empty_rows = [2, 31, 45, 46, 53, 55, 57]
    date_rows = [3]
    float_rows = [47, 48, 49, 50]
    string_rows = [51, 52, 54, 58, 59, 62]

    # 54
    # ('uzel', 'místníkomunikace', 'sledovanákomunikace', 'účelovákomunikace', '', 'dálnice', 'silnice3.třídy', 'silnice2.třídy', 'silnice1.třídy')

    # 58
    # ('', 'Souhlasnýsesměremúseku', 'Opačnýkesměruúseku', 'Souhlasný_se_směrem_úseku')

    # 59
    # ('', 'Pomalý', 'Velmirychlý', 'Rychlý', 'Kolektor', 'Odbočovacívpravo', 'Připojovacívpravo', 'Připojovacívlevo', 'Propomalávozidla', 'Odbočovacívlevo', 'Řadicí', 'Řadicívpravo')

    # 62
    # ('', 'Pomalý', 'Velmirychlý', 'Rychlý', 'Kolektor', 'Odbočovacívpravo', 'Připojovacívpravo', 'Připojovacívlevo', 'Propomalávozidla', 'Odbočovacívlevo', 'Řadicí', 'Řadicívpravo')

    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="ataf", cache_filename="ata_{}.pkl.gz"):
        """
        :param url: from which URL will be data downloaded
        :param folder: folder for tmp data
        :param cache_filename: file name in 'folder' with data processed with get_list
        """

        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename

        self.original_2 = tuple()

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
        zip_file = path.split(file_name)[-1]
        zip_file = self.correct_irregular_file_names(zip_file)
        zip_file = path.join(self.folder, zip_file)
        full_url = path.join(self.url, file_name)

        if path.isfile(zip_file): return

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
            html_response = s.get(self.url, headers=self.headers)
            for file_names in self.iter_file_names(html_response):
                self.download_file(file_names)

    def parse_region_data(self, region):
        """
        Fnc parses specified 'region' of downloaded data . If data are not downloaded, it downloads them.
        :param region: region code (3 chars)
        :return: ??
        """
        if region not in regions.keys(): return None

        self.download_data()

        region_file = regions[region] + '.csv'
        region_path = path.join(self.folder, region_file)
        downloaded_zips = [path.join(self.folder, zip_file) for zip_file in os.listdir(self.folder)
                           if zip_file.endswith('.zip')]

        for d_zip in downloaded_zips:
            try:
                with zipfile.ZipFile(d_zip) as zf:
                    print(self.original_2)
                    if region_file not in zf.namelist(): continue

                    with zf.open(region_file, 'r') as csv_f:

                        csv_reader = csv.reader(TextIOWrapper(csv_f, 'windows-1250'), delimiter=';', quotechar='"')
                        for row in csv_reader:
                            
                            # str_list = list(str(row[i]) for i in self.string_rows)
                            # print(str_list)


            except zipfile.BadZipFile as e:
                logging.warning(f'zip file {d_zip} is broken.')

    def get_list(self, regions=None):
        """
        Fnc returns data parsed by parse_region_data
        :param regions: list of selected regions, None means all regions
        :return:
        """
        ...


if __name__ == '__main__':
    # todo try download multiple times

    d = DataDownloader()
    for code in regions:
        d.parse_region_data(code)


    # with open('00.csv', 'rb') as cf:
    #     reader = csv.reader(TextIOWrapper(cf, encoding='windows-1250'), delimiter=';', quotechar='"')
    #     for row in reader:
    #         print(row)
