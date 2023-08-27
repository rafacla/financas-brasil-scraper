from datetime import datetime

from database.database import engine, Base, get_db
import scrapy

import parameters
from fundos_scraper.items import FundosScraperItem


class MesesSpider(scrapy.Spider):
    name = "cotas"
    start_urls = ['https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/']

    def __init__(self):
        # Create cursor, used to execute commands
        self.conn = next(get_db()).connection()
        self.cur = self.conn.connection.cursor()

    def parse(self, response):
        # Recupera links disponíveis na pagina da CVM
        datas = response.xpath('//pre/text()').getall()
        datas[:] = [desc.strip() for desc in datas]
        datas[:] = [desc[:11] for desc in datas]
        datas[:] = [datetime.strptime(desc, '%d-%b-%Y') for desc in datas if desc != '']
        links = response.css('a::attr(href)').getall()
        del links[0:2]
        # Recupera no banco de dados últimos arquivos atualizados
        self.cur.execute("select * from `"+parameters.scrapy_quotes_table_name+"`")
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
                item = FundosScraperItem()
                data_atualizacao = datas[i].strftime('%Y-%m-%d')
                item['data_atualizacao'] = data_atualizacao
                item['pipeline'] = 'meses'
                item['file_urls'] = [file_url]
                yield item
