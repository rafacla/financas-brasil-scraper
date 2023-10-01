from datetime import datetime

import scrapy

from src.database.database import get_db
from src.database.models import Scrapy_Fundos_Descricao
from fundos_scraper.items import FundosScraperDescItem


class MesesSpider(scrapy.Spider):
    name = "fundos_cvm_laminas"
    start_urls = ['https://dados.cvm.gov.br/dados/FI/DOC/LAMINA/DADOS/']

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
        self.cur.execute("select * from `"+Scrapy_Fundos_Descricao.__tablename__+"`")
        db_items = self.cur.fetchall()
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
                while j < len(db_items):
                    if db_items[j][1] == links[i] and datetime.strptime(db_items[j][2],'%Y-%m-%d').date() >= datas[i].date():
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
