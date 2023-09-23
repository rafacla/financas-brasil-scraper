import math
from datetime import datetime

import scrapy

import parameters
from database.database import get_db
from fundos_scraper.items import CetipDIItem


class MesesSpider(scrapy.Spider):
    name = "cetip_di"
    start_urls = ['https://www2.cetip.com.br/ConsultarTaxaDi/ConsultarTaxaDICetip.aspx']

    def __init__(self):
        # Create cursor, used to execute commands
        self.conn = next(get_db()).connection()
        self.cur = self.conn.connection.cursor()

    def parse(self, response):
        # Recupera links disponíveis na pagina da CVM
        jsonresponse = response.json()
        taxa = float(jsonresponse["taxa"].replace(",", "."))
        dataTaxa = datetime.strptime(jsonresponse["dataTaxa"], '%d/%m/%Y')
        taxaDiaria = math.pow(taxa / 100 + 1, 1 / 252)
        item = CetipDIItem()
        item['taxaDIAnual'] = str(taxa)
        item['taxaDIDiaria'] = str(taxaDiaria)
        item['dataTaxaDI'] = datetime.strftime(dataTaxa, '%Y-%m-%d')
        # Recupera no banco de dados últimos arquivos atualizados
        sql = "REPLACE INTO `" + parameters.taxa_di_table_name + \
            "` (`dataDI`, `taxaDIAnual`, `taxaDIDiaria`) VALUES (?, ?, ?);"
        self.cur.execute(sql, (item['dataTaxaDI'] , item['taxaDIAnual'], item['taxaDIDiaria']))
        self.conn.commit()
        yield item
