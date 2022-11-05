from datetime import datetime

import mysql.connector
import scrapy

import database_credentials
from fundos_scraper.items import FundosScraperItem


class MesesSpider(scrapy.Spider):
    name = "meses"
    start_urls = ['https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/']

    def __init__(self):
        self.conn = mysql.connector.connect(
            host=database_credentials.host[0],
            user=database_credentials.user[0],
            password=database_credentials.password[0],
            database=database_credentials.database[0]
        )
        # Create cursor, used to execute commands
        self.cur = self.conn.cursor()

    def parse(self, response):
        # Recupera links disponíveis na pagina da CVM
        datas = response.xpath('//pre/text()').getall()
        datas[:] = [desc.strip() for desc in datas]
        datas[:] = [desc[:11] for desc in datas]
        datas[:] = [datetime.strptime(desc, '%d-%b-%Y') for desc in datas if desc != '']
        links = response.css('a::attr(href)').getall()
        del links[0:2]
        # Recupera no banco de dados últimos arquivos atualizados
        self.cur.execute("select * from `scrapy_fundos_cvm`")
        mysql_items = self.cur.fetchall()
        # Agora iremos comparar quais itens já estão atualizados
        i = 0
        j = 0
        while i < len(links):
            while j < len(mysql_items):
                if mysql_items[j][1] == links[i] and mysql_items[j][2] >= datas[i].date():
                    links.pop(i)
                    datas.pop(i)
                j += 1
            i += 1
            j = 0

        for i, link in enumerate(links):
            if link[-3:] == 'zip':
                file_url = response.urljoin(link)
                item = FundosScraperItem()
                item['file_urls'] = [file_url]
                yield item
