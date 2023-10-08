import math
from datetime import datetime
from typing import Iterable

import numpy as np
import pandas as pd
import scrapy
from scrapy.http import Request

import database.models as Models
from fundos_scraper.items import CetipDIItem
from database.database import get_db


class MesesSpider(scrapy.Spider):
    name = "cetip_di"

    def __init__(self):
        # Retrieve all the existing dates in database - we don't need to update them
        self.conn = next(get_db()).connection()
        self.cur = self.conn.connection.cursor()
        self.cur.execute("select `dataDI` from `" + Models.TaxaDI.__tablename__ + "`")
        db_items = self.cur.fetchall()

        # Create an array with all the business days since year 2000 until today
        bdays = pd.bdate_range(start="2015-01-01", end=datetime.today()).strftime(
            "%Y-%m-%d"
        )

        # Now we are going to remove from our scraping the dates we already have in DB:
        for item in db_items:
            if item[0] in bdays:
                bdays = bdays.drop(item[0])

        self.cetip_dates = pd.DataFrame(
            np.transpose([bdays, np.ones(len(bdays)), np.ones(len(bdays))]),
            columns=["dataDI", "taxaDIAnual", "taxaDIDiaria"],
        )

    def start_requests(self):
        for index, row in self.cetip_dates.iterrows():
            yield Request(
                url="ftp://ftp.cetip.com.br/MediaCDI/"
                + row["dataDI"].replace("-", "")
                + ".txt",
            errback=self.parse,
            )

    def parse(self, response):
        if hasattr(response, "value") and response.value.response.status == 404:
            url = response.value.response.url
            taxaDIAnual = 1.
            taxaDiaria = 1.
        else:
            url = response.url
            taxaDIAnual = int(response.text) / 100
            taxaDiaria = math.pow(taxaDIAnual / 100 + 1, 1 / 252)
        dataDI = (
            url[-12:][:4]
            + "-"
            + url[-12:][4:6]
            + "-"
            + url[-12:][6:8]
        )
        item = CetipDIItem()
        item["taxaDIAnual"] = str(taxaDIAnual)
        item["taxaDIDiaria"] = str(taxaDiaria)
        item["dataTaxaDI"] = dataDI

        # Recupera no banco de dados últimos arquivos atualizados
        sql = (
            "INSERT INTO `"
            + Models.TaxaDI.__tablename__
            + "` (`dataDI`, `taxaDIAnual`, `taxaDIDiaria`) VALUES (?, ?, ?);"
        )
        self.cur.execute(
            sql, (item["dataTaxaDI"], item["taxaDIAnual"], item["taxaDIDiaria"])
        )
        self.conn.commit()
        yield item
