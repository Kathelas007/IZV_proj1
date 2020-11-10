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
    string_rows = [51, 52, 53, 54, 55, 58, 59, 62]
    int_rows = [0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
                29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 56, 57, 60, 61, 63]
    float_rows = [45, 46, 47, 48, 49, 50]

    column_names = ['p2a', 'h', 'i', 'j', 'k', 'l', 'p', 'q', 't', 'p1', 'p36', 'p37', 'weekday(p2a)', 'p2b', 'p6',
                    'p7', 'p8', 'p9', 'p10', 'p11', 'p12', 'p13a', 'p13b', 'p13c', 'p14', 'p15', 'p16', 'p17', 'p18',
                    'p19', 'p20', 'p21', 'p22', 'p23', 'p24', 'p27', 'p28', 'p34', 'p35', 'p39', 'p44', 'p45a', 'p47',
                    'p48a', 'p49', 'p50a', 'p50b', 'p51', 'p52', 'p53', 'p55a', 'p57', 'p58', 'n', 'o', 'r', 's', 'p5a',
                    'a', 'b', 'd', 'e', 'f', 'g', 'reg']

    column_description = ['rok mesic den', 'h', 'i', 'j', 'k', 'l', 'p', 'q', 't', 'id', 'druh pozemni komunikace',
                          'cislo pozemni komunikace', 'den v tydnu', 'cas', 'druh nehody',
                          'druh strazky s jedoucim vozidlem', 'druh pevne prekasky', 'charakter nehody',
                          'zavineni nehody', 'alkohol vinika', 'hlavni pricina', 'usmrceno', 'tezce zraneno',
                          'lehce zraneno', 'skoda', 'druh povrchu', 'stav povrchu', 'stav komunikace',
                          'povetrnostni podminky', 'viditelnost', 'rozhledove podminky', 'deleni komunikace',
                          'situovani nehody', 'rizeni provozu', 'mistni uprava prednosti', 'spec mista a objekty',
                          'smerove pomery', 'pocet zucatnenych vozidel', 'misto dopravni nehody',
                          'druh krizujici komunikace', 'druh vozidla', 'znacka mot vozidla', 'rok vyroby vozidla',
                          'charakter vozidla', 'smyk', 'vozidlo po nehode', 'unik hmot', 'zpusob vyprosteni',
                          'smer jizdy nebo pozastaveni voz', 'skoda na vozidle', 'kat ridice', 'stav ridice',
                          'vnejso ovlivneni', 'n', 'o', 'r', 's', 'lokalita nehody', 'a', 'b', 'd', 'e', 'f', 'g',
                          'reg']


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

    self.original_2 = dict()


def correct_irregular_file_names(self, file_name: str) -> str:
    """
    Zip names at URL are irregular. This fnc fices it.
    :param file_name: zip name
    """
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


def zip_row_generator(self, region, region_file, downloaded_zips):
    """
    Row generator. Fnc yields all rows form all zip files of specified region.
    :param region: region code
    :param region_file: file name with region
    :param downloaded_zips: names of all downloaded zips
    :return: row
    """
    for d_zip in downloaded_zips:
        try:
            with zipfile.ZipFile(d_zip) as zf:
                if region_file not in zf.namelist(): continue

                with zf.open(region_file, 'r') as csv_f:
                    csv_reader = csv.reader(TextIOWrapper(csv_f, 'windows-1250'), delimiter=';', quotechar='"')
                    for row in csv_reader:
                        yield self.clean_row_data(row, region)

        except zipfile.BadZipFile as e:
            logging.warning(f'zip file {d_zip} is broken.')


def clean_numpy_data(self, data_list, region):
    """
    Fnc sets proper data type and cleans data. At the end adds region
    :param data_list: all data from one region
    :return: cleaned numpy array
    """
    data = np.array(data_list).T

    d_time = np.array(data[self.date_rows], dtype=np.datetime64)
    d_str = np.array(data[self.string_rows], dtype=str)

    data_floats = np.where(data[self.float_rows] == "", -1, data[self.float_rows])
    d_float = np.array(data_floats.astype(float) * 100, dtype=int)

    data_ints = np.where(data[self.int_rows] == "", -1, data[self.int_rows])
    d_int = np.array(data_ints, dtype=int)

    region_row = np.full((len(data[0])), region)

    return np.array((*d_time, *d_str, *d_int, *d_float, region_row), dtype=object).T


def clean_row_data(self, row: list, reg):
    """
    Basic clean on one row.
    :return: cleaned row
    """
    row[3] = np.datetime64(row[3])
    try:
        row[34] = int(row[34])
    except ValueError as e:
        row[34] = -1

    for r_id in self.float_rows:
        try:
            row[r_id] = row[r_id].replace(',', '.')
        except ValueError as e:
            row[r_id] = None
    row.append(reg)
    return row


def parse_region_data(self, region):
    """
    Fnc parses specified 'region' of downloaded data . If data are not downloaded, it downloads them.
    :param region: region code (3 chars)
    :return: tuple(list(columns names), numpy(region data))
    """

    if region not in region_codes.keys(): return None

    # check downloaded zips
    downloaded_zips = [zip_file for zip_file in os.listdir(self.folder)
                       if zip_file.endswith('.zip')]
    if len(downloaded_zips) == 0:
        self.download_data()

    # get zip names
    region_file = region_codes[region] + '.csv'
    region_path = path.join(self.folder, region_file)
    downloaded_zips = [path.join(self.folder, zip_file) for zip_file in os.listdir(self.folder)
                       if zip_file.endswith('.zip')]

    # get all rows as numpy
    data_list = [row for row in self.zip_row_generator(region, region_file, downloaded_zips)]
    data_numpy = self.clean_numpy_data(data_list, region)

    return tuple([self.column_names, data_numpy])


def get_list(self, regs=None):
    """
    Fnc returns data parsed by parse_region_data. Data are cached in 'folder' and in attribute cached_regions.
    If any of cache is available, it will be used instead of parse_region_data.
    :param regs: list of selected regions, None means all regions
    :return: tuple(list(columns names), numpy(region data))
    """

    if regs is None:
        regs = region_codes.keys()

    if not all([r in region_codes for r in regs]):
        return None

    return_list = list()
    for reg in regs:
        reg_file = path.join(self.folder, self.cache_filename.format(reg))

        # cached in class
        if reg in self.cached_regions.keys():
            return_list.append(self.cached_regions[reg][1])

        # cached in 'folder'
        elif path.isfile(reg_file):
            with gzip.open(reg_file, 'rb') as gz:
                pck = pickle.load(gz)
                return_list.append(pck)

        # not cached
        else:
            # get data
            curr_zip_data = self.parse_region_data(reg)
            return_list.append(curr_zip_data[1])

            # cache it
            self.cached_regions[reg] = curr_zip_data[1]
            with gzip.open(reg_file, 'wb') as gz:
                pickle.dump(curr_zip_data[1], gz)

    return tuple([self.column_names, *return_list])


def unique_data(self, data, ids=None):
    """
    Analysis function. It returns unique data in givan indexes
    :param data: region data
    :param ids: column ids
    :return: unique values
    """
    data = data.T

    if ids is None:
        return [np.unique(data[i]) for i in range(0, len(data))]

    return [np.unique(data[i]) for i in ids]


if __name__ == '__main__':
    d = DataDownloader()

    regions = ["PHA", "JHM", "OLK"]
    if len(sys.argv) == 4:
        regions = sys.argv[1:]

    l = d.get_list(regions)

    print('regions:\t\t', regions)
    print('row number:\t\t', len(l[1]))
    print('column number:\t', len(l[1][0]))
    print('columns:\t\t', l[0])
    print('column description:\t\t', d.column_description)
