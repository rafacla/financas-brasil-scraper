from datetime import datetime

import mysql.connector
import scrapy

import parameters
from fundos_scraper.items import FundosScraperDescItem


class MesesSpider(scrapy.Spider):
    name = "laminas"
    start_urls = ['https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/']

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
        datas = response.xpath('//pre/text()').getall()
        datas[:] = [desc.strip() for desc in datas]
        datas[:] = [desc[:11] for desc in datas]
        datas[:] = [datetime.strptime(desc, '%d-%b-%Y') for desc in datas if desc != '']
        links = response.css('a::attr(href)').getall()
        del links[0:2]
        # Recupera no banco de dados últimos arquivos atualizados
        self.cur.execute("select * from `"+parameters.scrapy_description_table_name+"`")
        mysql_items = self.cur.fetchall()
        # Agora iremos comparar quais itens já estão atualizados
        i = 0
        j = 0
        k = 0
        while k < len(links):
            if links[k][-3:] != 'zip':
                try:
                    links.pop(k)
                    if k < len(datas):
                        datas.pop(k)
                    k = 0
                except:
                    pass
            k += 1
        try:
            while i < len(links):
                while j < len(mysql_items):
                    if mysql_items[j][1] == links[i] and mysql_items[j][2] >= datas[i].date():
                        links.pop(i)
                        datas.pop(i)
                        i = 0
                        j = 0
                    else:
                        j += 1
                i += 1
                j = 0
        except:
            pass
        for i, link in enumerate(links):
            if link[-3:] == 'zip':
                file_url = response.urljoin(link)
                item = FundosScraperDescItem()
                data_atualizacao = datas[i].strftime('%Y-%m-%d')
                item['data_atualizacao'] = data_atualizacao
                item['file_urls'] = [file_url]
                item['pipeline'] = 'laminas'
                yield item
