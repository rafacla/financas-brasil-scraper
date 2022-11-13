import math
from datetime import datetime

import mysql.connector
import scrapy

import parameters
from fundos_scraper.items import CetipDIItem


class MesesSpider(scrapy.Spider):
    name = "cetip_di"
    start_urls = ['https://www2.cetip.com.br/ConsultarTaxaDi/ConsultarTaxaDICetip.aspx']

    def __init__(self):
        self.conn = mysql.connector.connect(
            host=parameters.host,
            user=parameters.user,
            password=parameters.password,
            database=parameters.database
        )
        # Create cursor, used to execute commands
        self.cur = self.conn.cursor()

    def parse(self, response):
        # Recupera links disponíveis na pagina da CVM
        jsonresponse = response.json()
        taxa = float(jsonresponse["taxa"].replace(",", "."))
        dataTaxa = datetime.strptime(jsonresponse["dataTaxa"], '%d/%m/%Y')
        taxaDiaria = math.pow(taxa, 1 / 252)
        item = CetipDIItem()
        item['taxaDIAnual'] = str(taxa)
        item['taxaDIDiaria'] = str(taxaDiaria)
        item['dataTaxaDI'] = datetime.strftime(dataTaxa, '%Y-%m-%d')
        # Recupera no banco de dados últimos arquivos atualizados
        values = (item['dataTaxaDI'], item['taxaDIAnual'], item['taxaDIDiaria'])
        self.cur.execute(
            "REPLACE INTO `" + parameters.taxa_di_table_name +
            "` (`dataDI`, `taxaDIAnual`, `taxaDIDiaria`) VALUES (%s, %s, %s);", values)
        self.conn.commit()
        yield item
