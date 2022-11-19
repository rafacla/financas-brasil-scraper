from datetime import datetime

import mysql.connector
import scrapy

import parameters
from fundos_scraper.items import TesouroDiretoItem


class MesesSpider(scrapy.Spider):
    name = "tesouro"
    start_urls = ['https://www.tesourotransparente.gov.br/ckan/dataset/df56aa42-484a-4a59-8184-7676580c81e3']

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
        links = response.css('a::attr(href)').getall()
        for i, link in enumerate(links):
            if link[-3:] == 'csv':
                file_url = response.urljoin(link)
                item = TesouroDiretoItem()
                item['pipeline'] = 'tesouro'
                item['file_urls'] = [file_url]
                yield item
