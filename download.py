import csv
import gzip
import logging
import os
import pathlib
import pickle
import zipfile
from io import TextIOWrapper
from os import path

import numpy as np
import requests
from bs4 import BeautifulSoup
import sys

region_codes = {
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

    date_rows = [3]
    float_rows = [47, 48, 49, 50]
    string_rows = [51, 52, 54, 58, 59, 62]

    column_names = ['reg', 'p1', 'p36', 'p37', 'p2a', 'weekday(p2a)', 'p2b', 'p6', 'p7', 'p8', 'p9', 'p10', 'p11',
                    'p12',
                    'p13a', 'p13b', 'p13c', 'p14', 'p15', 'p16', 'p17', 'p18', 'p19', 'p20', 'p21', 'p22', 'p23',
                    'p24', 'p27', 'p28', 'p34', 'p35', 'p39', 'p44', 'p45a', 'p47', 'p48a', 'p49', 'p50a', 'p50b',
                    'p51', 'p52', 'p53', 'p55a', 'p57', 'p58', 'a', 'b', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                    'n', 'o', 'p', 'q', 'r', 's', 't', 'p5a']

    column_description = ['reg', 'id', 'druh pozemni komunikace', 'cislo pozemni komunikace', 'rok mesic den',
                          'den v tydnu',
                          'cas', 'druh nehody', 'druh strazky s jedoucim vozidlem', 'druh pevne prekasky',
                          'charakter nehody', 'zavineni nehody', 'alkohol vinika', 'hlavni pricina', 'usmrceno',
                          'tezce zraneno', 'lehce zraneno', 'skoda', 'druh povrchu', 'stav povrchu', 'stav komunikace',
                          'povetrnostni podminky', 'viditelnost', 'rozhledove podminky', 'deleni komunikace',
                          'situovani nehody', 'rizeni provozu', 'mistni uprava prednosti', 'spec mista a objekty',
                          'smerove pomery', 'pocet zucatnenych vozidel', 'misto dopravni nehody',
                          'druh krizujici komunikace', 'druh vozidla', 'znacka mot vozidla', 'rok vyroby vozidla',
                          'charakter vozidla', 'smyk', 'vozidlo po nehode', 'unik hmot',
                          'smer jizdy nebo pozastaveni voz', 'skoda na vozidle', 'kat ridice', 'stav ridice',
                          'vnejsi ovlivneni', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                          'lokalita nehody'
                          ]

    # 54
    # ('uzel', 'místníkomunikace', 'sledovanákomunikace', 'účelovákomunikace', '', 'dálnice', 'silnice3.třídy', 'silnice2.třídy', 'silnice1.třídy')

    # 58
    # ('', 'Souhlasnýsesměremúseku', 'Opačnýkesměruúseku', 'Souhlasný_se_směrem_úseku')

    # 59
    # ('', 'Pomalý', 'Velmirychlý', 'Rychlý', 'Kolektor', 'Odbočovacívpravo', 'Připojovacívpravo', 'Připojovacívlevo', 'Propomalávozidla', 'Odbočovacívlevo', 'Řadicí', 'Řadicívpravo')

    # 62
    # ('', 'Pomalý', 'Velmirychlý', 'Rychlý', 'Kolektor', 'Odbočovacívpravo', 'Připojovacívpravo', 'Připojovacívlevo', 'Propomalávozidla', 'Odbočovacívlevo', 'Řadicí', 'Řadicívpravo')

    origo = tuple()

    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="ataf", cache_filename="ata_{}.pkl.gz"):
        """
        :param url: from which URL will be data downloaded
        :param folder: folder for tmp data
        :param cache_filename: file name in 'folder' with data processed with get_list
        """

        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename
        self.cached_regions = dict()

        self.original_2 = tuple()

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

    def iter_file_names(self, html_response):
        """
        Fnc finds html table element and iterates throught its zip file names
        :return: current file name
        """

        soup = BeautifulSoup(html_response.text, 'html.parser')
        table_el = soup.find("table", {'class': 'table table-fluid'})

        last_file = None
        for tr_el in table_el.find_all('a', {'class': ['btn', 'btm-sm', 'btm-primary']}):
            href_zip = tr_el['href']
            if not href_zip.endswith('zip'): continue

            corrected = self.correct_irregular_file_names(href_zip[5:])
            if corrected.startswith('datagis-12'):
                yield href_zip
            else:
                last_file = href_zip

        yield last_file

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

    def insert_reg(self, row, reg):
        row.insert(0, reg)
        return row

    def get_row_list(self, region, region_file, downloaded_zips):
        data_list = list()
        one_zip_list = list()
        for d_zip in downloaded_zips:
            try:
                with zipfile.ZipFile(d_zip) as zf:
                    if region_file not in zf.namelist(): continue

                    with zf.open(region_file, 'r') as csv_f:
                        csv_reader = csv.reader(TextIOWrapper(csv_f, 'windows-1250'), delimiter=';', quotechar='"')
                        one_zip_list = list([region] + row for row in csv_reader)
                        data_list += one_zip_list

            except zipfile.BadZipFile as e:
                logging.warning(f'zip file {d_zip} is broken.')

        return data_list

    def parse_region_data(self, region):
        """
        Fnc parses specified 'region' of downloaded data . If data are not downloaded, it downloads them.
        :param region: region code (3 chars)
        :return: list()
        """
        if region not in region_codes.keys(): return None

        # check downloaded zips
        downloaded_zips = [zip_file for zip_file in os.listdir(self.folder)
                           if zip_file.endswith('.zip')]
        if len(downloaded_zips) == 0:
            self.download_data()

        region_file = region_codes[region] + '.csv'
        region_path = path.join(self.folder, region_file)
        downloaded_zips = [path.join(self.folder, zip_file) for zip_file in os.listdir(self.folder)
                           if zip_file.endswith('.zip')]

        # todo clean
        data_list = self.get_row_list(region, region_file, downloaded_zips)
        data_numpy = np.array(data_list)

        return tuple([self.column_names, data_numpy])

    def get_list(self, regions=None):
        """
        Fnc returns data parsed by parse_region_data
        :param regions: list of selected regions, None means all regions
        :return:
        """

        if not all([r in region_codes for r in regions]):
            return None

        if regions is None:
            regions = region_codes

        return_list = list()
        for reg in regions:
            reg_file = path.join(self.folder, self.cache_filename.format(reg))

            if reg in self.cached_regions.keys():
                return_list.append(self.cached_regions[reg][1])

            elif path.isfile(reg_file):
                with gzip.open(reg_file, 'rb') as gz:
                    pck = pickle.load(gz)
                    return_list.append(pck)

            else:
                curr_zip_data = self.parse_region_data(reg)
                self.cached_regions[reg] = curr_zip_data[1]
                return_list.append(curr_zip_data[1])
                with gzip.open(reg_file, 'wb') as gz:
                    pickle.dump(curr_zip_data[1], gz)

        return tuple([self.column_names, *return_list])

    def fnc(l):
        print(id(l))


if __name__ == '__main__':
    # todo try download multiple times
    # todo comments
    d = DataDownloader()

    regions = ["PHA", "JHM", "OLK"]
    if len(sys.argv) == 4:
        regions = sys.argv[1:]

    l = d.get_list(regions)

    print('regions:\t', regions)
    print('row number:\t', len(l[1]))
    print('columns:\t', l[0])
    print('column description:\t', d.column_description)
