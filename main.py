import re
from copy import deepcopy
from datetime import datetime
from pprint import pprint

import pandas as pd
import requests
from bs4 import BeautifulSoup

urls = {
    'Nepal Stok': {
        'url': 'http://www.nepalstock.com/company/display/{number}',
        'table_identifier': lambda tag: tag.name == 'table',
        'regex': '{number}',
        'valid_data': {
            'Change (Rs.) and (%)': '% Change',
            'Total Listed Shares': 'Total Listed Shares',
            'Paid Up Value (Rs.)': 'Paid Up Values',
            'Total Paid Up Value (Rs.)': 'Total Paid Up Value',
            'Market Capitalization (Rs.)': 'Market Capitalization',
            'Last Traded Price (Rs.)': 'Last Trade Price'
        }

    },
    'Nepali Paisa': {
        'url': 'http://www.nepalipaisa.com/CompanyDetail.aspx/{symbol}/?quote={symbol}',
        'table_identifier': lambda tag: tag.name == 'table',
        'regex': '{symbol}',
        'valid_data': {
            '% Change': '% Change',
            'Bonus Share': '% Bonus',
            # 'Bonuse Distribution Date': 'Bonus Distribution Date',
            'Cash Dividend': '% Dividend',
            # 'Day TurnOver': 'Day TurnOver',
            'Market Capitalization (Rs.)': 'Market Capitalization',
            'Total Listed Shares': 'Total Listed Shares',
            'Total Paid Up Value': 'Total Paid Up Value',
            'Previous Close': 'Last Trade Price'
        }
    },
    'Mero Lagani': {
        'url': 'https://merolagani.com/CompanyDetail.aspx?symbol={symbol}',
        'table_identifier': lambda tag: tag.name == 'table' and tag.has_attr('id') and tag['id'] == 'accordion',
        'regex': '{symbol}',
        'valid_data': {
            'Shares Outstanding': 'Total Listed Shares',
            'Market Price': 'Last Trade Price',
            '% Change': '% Change',
            'Book Value': 'Paid Up Values',
            '% Dividend': '% Dividend',
            '% Bonus': '% Bonus',
            'Market Capitalization': 'Market Capitalization'
        }

    }
}

OPTIONS = {'ACLBSL': '2790', 'ADBL': '397', 'AHPC': '360', 'AIL': '2893', 'AKBSL': '2845', 'AKBSLP': '2846',
           'AKJCL': '2788', 'AKPL': '2757', 'ALBSL': '2807', 'ALICL': '385', 'ALICLP': '599',
           'AMFI': '2777', 'AMFIPO': '2778', 'API': '697', 'AVU': '211', 'BARUN': '686', 'BBC': '156',
           'BFC': '227', 'BFCPO': '339', 'BFLPO': '440', 'BHBL': '487', 'BHBLPO': '631', 'BNL': '195',
           'BNT': '213', 'BOKD2079': '2870', 'BOKL': '138', 'BOKLPO': '289', 'BPCL': '153', 'BSBLPO': '628',
           'BSL': '217', 'BSM': '205', 'CBBL': '164', 'CBBLPO': '698', 'CBL': '532', 'CBLPO': '653',
           'CCBL': '605', 'CCBLPO': '656', 'CEFL': '296', 'CEFLPO': '2756', 'CFCL': '245', 'CFCLPO': '676',
           'CFL': '361', 'CHCL': '154', 'CHL': '2766', 'CIT': '210', 'CIZBD86': '2889', 'CLBSL': '693',
           'CMB': '259', 'CMBFLP': '288', 'CMF1': '2780', 'CMF2': '2862', 'CORBL': '450', 'CORBLP': '2763',
           'CZBIL': '348', 'CZBILP': '493', 'DBBL': '311', 'DBBLPO': '573', 'DDBL': '166', 'DDBLPO': '607',
           'DHPL': '2754', 'EBL': '137', 'EBLCP': '277', 'EBLD2078': '2871', 'EBLPO': '594', 'EDBL': '274',
           'EDBLPO': '612', 'EIC': '181', 'EICPO': '702', 'FBBLPO': '319', 'FHL': '228', 'FMDBL': '490',
           'FMDBLP': '722', 'FOWAD': '2758', 'GBBL': '417', 'GBBLPO': '623', 'GBD80/81': '2840',
           'GBIME': '341', 'GBIMEP': '511', 'GBLBS': '583', 'GBLBSP': '712', 'GDBL': '420', 'GDBLPO': '670',
           'GFCL': '232', 'GFCLPO': '355', 'GGBSL': '2852', 'GHL': '2806', 'GIC': '2905', 'GILB': '705',
           'GILBPO': '731', 'GIMES1': '1740', 'GLBSL': '2826', 'GLICL': '447', 'GLICLP': '621',
           'GMFBS': '2815', 'GMFIL': '263', 'GMFILP': '613', 'GRDBL': '2744', 'GRDBLP': '2745',
           'GRU': '207', 'GUFL': '204', 'GUFLPO': '315', 'HAMRO': '576', 'HAMROP': '639', 'HATH': '421',
           'HATHPO': '685', 'HBL': '134', 'HBLD83': '2873', 'HBLPO': '564', 'HBT': '215', 'HDHPC': '2880',
           'HDL': '235', 'HFL': '441', 'HGI': '179', 'HGIPO': '700', 'HIDCL': '2742', 'HPPL': '2767',
           'HURJA': '2824', 'ICFC': '273', 'ICFCPO': '400', 'IGI': '186', 'IGIPO': '635', 'ILBS': '2832',
           'ILFCMP': '725', 'JBBL': '418', 'JBBLPO': '579', 'JBNL': '496', 'JBNLPO': '654', 'JEFL': '577',
           'JEFLPO': '673', 'JFL': '250', 'JFLPO': '292', 'JOSHI': '2789', 'JSLBB': '695', 'JSLBBP': '719',
           'JSM': '209', 'KADBL': '505', 'KADBLP': '710', 'KBL': '142', 'KBLD86': '2885', 'KBLPO': '283',
           'KEBL': '427', 'KEBLPO': '729', 'KKHC': '2751', 'KMCDB': '593', 'KMCDBP': '678', 'KNBL': '419',
           'KNBLPO': '637', 'KPCL': '2787', 'KRBL': '428', 'KRBLPO': '1739', 'KSBBL': '459',
           'KSBBLP': '677', 'LBBL': '358', 'LBBLPO': '626', 'LBL': '141', 'LBLPO': '286', 'LEC': '2903',
           'LEMF': '2765', 'LFC': '231', 'LFCPO': '330', 'LGIL': '190', 'LGILPO': '703', 'LICN': '188',
           'LICNPO': '696', 'LLBS': '618', 'LLBSPO': '644', 'LUK': '2902', 'LVF1': '674', 'MBL': '140',
           'MBLPO': '281', 'MDB': '371', 'MDBLPO': '291', 'MDBPO': '609', 'MEGA': '562', 'MEGAPO': '657',
           'MERO': '1741', 'MEROPO': '1742', 'MFIL': '516', 'MFILPO': '730', 'MHNL': '2811', 'MLBBL': '601',
           'MLBBLP': '707', 'MLBL': '401', 'MLBLPO': '620', 'MMFDB': '682', 'MMFDBP': '711', 'MNBBL': '474',
           'MNBBLP': '640', 'MPFL': '471', 'MPFLPO': '1737', 'MSLB': '2768', 'MSMBS': '691',
           'MSMBSP': '1736', 'NABBC': '172', 'NABIL': '131', 'NABILP': '282', 'NADEP': '2784',
           'NADEPP': '2785', 'NAGRO': '2816', 'NBB': '136', 'NBBD2085': '2854', 'NBBL': '602',
           'NBBLPO': '649', 'NBBPO': '380', 'NBBU': '233', 'NBF2': '2835', 'NBL': '517', 'NBLD82': '2892',
           'NCCB': '144', 'NCCBPO': '327', 'NCDB': '598', 'NCDBPO': '699', 'NEF': '2753', 'NFD': '151',
           'NFS': '194', 'NFSPO': '651', 'NGPL': '2743', 'NHDL': '2769', 'NHPC': '152', 'NIB': '132',
           'NIBD2082': '2851', 'NIBLPF': '2755', 'NIBPO': '469', 'NIBSF1': '636', 'NICA': '139',
           'NICAD 85/86': '2825', 'NICAD8182': '2895', 'NICAD8283': '2869', 'NICAP': '309', 'NICBF': '2863',
           'NICD83/84': '2868', 'NICGF': '2779', 'NICL': '176', 'NICLBSL': '2887', 'NICLPO': '689',
           'NIDC': '160', 'NIDCPO': '658', 'NIL': '183', 'NILPO': '615', 'NKU': '222', 'NLBBL': '396',
           'NLBBLP': '661', 'NLBSL': '694', 'NLBSLP': '733', 'NLG': '559', 'NLGPO': '684', 'NLIC': '187',
           'NLICL': '178', 'NLICLP': '582', 'NLICP': '589', 'NLO': '198', 'NMB': '238', 'NMB50': '2867',
           'NMBD2085': '2850', 'NMBHF1': '2752', 'NMBMF': '704', 'NMBMFP': '723', 'NMBPO': '391',
           'NMFBS': '2746', 'NMFBSP': '2747', 'NRIC': '2881', 'NRN': '2898', 'NSEWA': '2781',
           'NSEWAP': '2782', 'NSM': '200', 'NSMPO': '313', 'NTC': '307', 'NTL': '158', 'NUBL': '163',
           'NUBLPO': '672', 'NVG': '201', 'NWC': '159', 'ODBL': '398', 'ODBLPO': '568', 'OHL': '149',
           'PBLD84': '2904', 'PBLD86': '2875', 'PCBL': '357', 'PCBLP': '544', 'PDBLPO': '346', 'PFL': '236',
           'PFLPO': '390', 'PIC': '182', 'PICL': '189', 'PICLPO': '648', 'PICPO': '713', 'PLIC': '393',
           'PLICPO': '701', 'PMHPL': '2786', 'PPCL': '2813', 'PRFLPO': '488', 'PRIN': '184',
           'PRINPO': '590', 'PROFL': '338', 'PROFLP': '354', 'PRVU': '255', 'PRVUPO': '632',
           'PSDBLP': '387', 'PURBL': '451', 'PURBLP': '714', 'RADHI': '2776', 'RBCL': '177',
           'RBCLPO': '634', 'RHPC': '610', 'RHPL': '2841', 'RJM': '203', 'RLFL': '587', 'RLFLPO': '650',
           'RLI': '2900', 'RMBFPO': '312', 'RMDC': '575', 'RMDCPO': '659', 'RRHP': '2783', 'RSDC': '2748',
           'RSDCP': '2749', 'SABSL': '2843', 'SADBL': '472', 'SADBLP': '708', 'SAEF': '2773',
           'SAND2085': '2828', 'SANIMA': '171', 'SAPDBL': '2860', 'SAPDBLP': '2861', 'SBBLJ': '174',
           'SBBLJP': '479', 'SBBLPO': '362', 'SBI': '135', 'SBIBD86': '2890', 'SBIPO': '347', 'SBL': '145',
           'SBLD2082': '2872', 'SBLD83': '2864', 'SBLD84': '2912', 'SBLPO': '449', 'SBPP': '226',
           'SCB': '133', 'SCBPO': '655', 'SDESI': '2764', 'SDLBSL': '2896', 'SEF': '2770', 'SEOS': '616',
           'SFC': '221', 'SFCL': '256', 'SFCLP': '298', 'SFCPO': '299', 'SFFIL': '261', 'SFFILP': '352',
           'SFMF': '2877', 'SGI': '2908', 'SHBL': '625', 'SHBLPO': '732', 'SHINE': '473', 'SHINEP': '669',
           'SHIVM': '2809', 'SHL': '147', 'SHPC': '591', 'SIC': '185', 'SICL': '192', 'SICLPO': '606',
           'SICPO': '619', 'SIFC': '244', 'SIFCPO': '456', 'SIGS2': '2859', 'SIL': '280', 'SILPO': '630',
           'SINDU': '561', 'SINDUP': '652', 'SJCL': '2842', 'SKBBL': '574', 'SKBBLP': '671', 'SLBBL': '545',
           'SLBBLP': '721', 'SLBS': '2750', 'SLBSL': '2804', 'SLBSP': '2911', 'SLICL': '403',
           'SLICLP': '581', 'SMATA': '2761', 'SMATAP': '2762', 'SMB': '2771', 'SMBPO': '2772',
           'SMFBS': '2829', 'SMFDB': '502', 'SMFDBP': '663', 'SNLB': '592', 'SNLBP': '643', 'SPARS': '2812',
           'SPDL': '2759', 'SRBL': '359', 'SRBLD83': '2878', 'SRBLPO': '522', 'SRD80': '2834', 'SRS': '230',
           'SSHL': '2907', 'STC': '155', 'SWBBL': '268', 'SWBBLP': '314', 'SYFL': '249', 'SYFLPO': '624',
           'TMDBL': '2855', 'TMDBLP': '2856', 'TRH': '148', 'TRHPR': '608', 'UFL': '242', 'UFLPO': '350',
           'UIC': '180', 'UICPO': '726', 'UMHL': '2760', 'UNHPL': '2831', 'UNL': '219', 'UPCL': '2810',
           'UPPER': '2792', 'USLB': '2774', 'USLBP': '2775', 'VLBS': '687', 'VLBSPO': '716', 'WOMI': '706',
           'WOMIPO': '720', 'YHL': '146'}


class InconsistentUrl(Exception):
    pass


class Extractor:
    url_response = None

    def __init__(self, url, table_identifier, regex, valid_data):
        self.url = url
        self.table_identifier = table_identifier
        self.regex = regex
        self.valid_data = valid_data

    def get_company_name(self, company_name):
        if 'number' in self.regex:
            return OPTIONS.get(company_name.upper())
        return company_name

    def connect(self, company_name):
        url_response = requests.get(
            self.url.replace(
                self.regex,
                self.get_company_name(company_name)
            )
        )
        if url_response.status_code == 200:
            self.url_response = url_response
        else:
            raise InconsistentUrl()

    def extract(self):
        soup = BeautifulSoup(self.url_response.content, 'html5lib')
        tables = soup.find_all(self.table_identifier)
        extracted_data = {}
        for table in tables:
            for tr in table.find_all():
                th = tr.find(lambda tag: tag.name == 'th')
                if not th:
                    td = tr.find_all(lambda tag: tag.name == 'td')
                    if len(td) == 2:
                        extracted_data[self.prettify_data(td[0].text)] = self.prettify_data(td[1].text)
                else:
                    td = tr.find(lambda tag: tag.name == 'td')
                    extracted_data[self.prettify_data(th.text)] = self.prettify_data(td.text) if td else None
        return self.clean_data(deepcopy(extracted_data))

    def clean_data(self, extracted_data):
        updated_data = {}
        for _key, _value in self.valid_data.items():
            updated_data[_value] = extracted_data.get(_key, None)
        return updated_data

    @staticmethod
    def prettify_data(extracted_text):
        return re.sub('[^A-Za-z0-9.%() ]+', '', extracted_text.strip())


if __name__ == '__main__':
    final_data = []
    symbol = input('Enter Company Symbol: ')
    if symbol.upper() not in OPTIONS:
        raise ValueError('Enter Valid company Symbol.')

    for key, value in urls.items():
        extractor = Extractor(**value)
        try:
            extractor.connect(symbol.upper())
            extracted_data = extractor.extract()
            extracted_data['Company'] = key
            final_data.append(extracted_data)
        except InconsistentUrl:
            print(f'Unable to build url for \'{key}\'')
        del extractor

    df = pd.DataFrame(final_data, urls.keys())
    df.to_csv(f'{datetime.now()}.csv', index=False, encoding='utf-8')
    pprint(final_data)
