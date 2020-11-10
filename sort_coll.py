column_description = ['id', 'druh pozemni komunikace', 'cislo pozemni komunikace', 'rok mesic den', 'den v tydnu',
                      'cas', 'druh nehody', 'druh strazky s jedoucim vozidlem', 'druh pevne prekasky',
                      'charakter nehody', 'zavineni nehody', 'alkohol vinika', 'hlavni pricina', 'usmrceno',
                      'tezce zraneno', 'lehce zraneno', 'skoda', 'druh povrchu', 'stav povrchu', 'stav komunikace',
                      'povetrnostni podminky', 'viditelnost', 'rozhledove podminky', 'deleni komunikace',
                      'situovani nehody', 'rizeni provozu', 'mistni uprava prednosti', 'spec mista a objekty',
                      'smerove pomery', 'pocet zucatnenych vozidel', 'misto dopravni nehody',
                      'druh krizujici komunikace', 'druh vozidla', 'znacka mot vozidla', 'rok vyroby vozidla',
                      'charakter vozidla', 'smyk', 'vozidlo po nehode', 'unik hmot', 'zpusob vyprosteni',
                      'smer jizdy nebo pozastaveni voz', 'skoda na vozidle', 'kat ridice', 'stav ridice',
                      'vnejso ovlivneni', 'a', 'b', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'n', 'o', 'p', 'q',
                      'r', 's', 't', 'lokalita nehody', 'reg'
                      ]
column_names = ['p1', 'p36', 'p37', 'p2a', 'weekday(p2a)', 'p2b', 'p6', 'p7', 'p8', 'p9', 'p10', 'p11',
                'p12',
                'p13a', 'p13b', 'p13c', 'p14', 'p15', 'p16', 'p17', 'p18', 'p19', 'p20', 'p21', 'p22', 'p23',
                'p24', 'p27', 'p28', 'p34', 'p35', 'p39', 'p44', 'p45a', 'p47', 'p48a', 'p49', 'p50a', 'p50b',
                'p51', 'p52', 'p53', 'p55a', 'p57', 'p58', 'a', 'b', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                'n', 'o', 'p', 'q', 'r', 's', 't', 'p5a', 'reg']

date_rows = [3]
string_rows = [51, 52, 53, 54, 55, 58, 59, 62]
int_rows = [0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 56, 57, 60, 61, 63]
float_rows = [45, 46, 47, 48, 49, 50]


# 54
# ('uzel', 'místníkomunikace', 'sledovanákomunikace', 'účelovákomunikace', '', 'dálnice', 'silnice3.třídy', 'silnice2.třídy', 'silnice1.třídy')

# 58
# ('', 'Souhlasnýsesměremúseku', 'Opačnýkesměruúseku', 'Souhlasný_se_směrem_úseku')

# 59
# ('', 'Pomalý', 'Velmirychlý', 'Rychlý', 'Kolektor', 'Odbočovacívpravo', 'Připojovacívpravo', 'Připojovacívlevo', 'Propomalávozidla', 'Odbočovacívlevo', 'Řadicí', 'Řadicívpravo')

# 62
# ('', 'Pomalý', 'Velmirychlý', 'Rychlý', 'Kolektor', 'Odbočovacívpravo', 'Připojovacívpravo', 'Připojovacívlevo', 'Propomalávozidla', 'Odbočovacívlevo', 'Řadicí', 'Řadicívpravo')


def sort_coll(coll: list):
    ret_list = list()

    ret_list = [coll[i] for i in date_rows + string_rows + int_rows + float_rows]
    ret_list.append('reg')
    return ret_list


print((sort_coll(column_description)))
