from datetime import datetime
from ftplib import FTP

import numpy as np
import pandas as pd
import scrapy
from scrapy.http import Request

import database.models as Models
from database.database import get_db
from fundos_scraper.items import CetipDIItem


class CetipDI(scrapy.Spider):
    name = "cetip_di"

    def __init__(self):
        # Retrieve all the existing dates in database - we don't need to update them
        self.conn = next(get_db()).connection()
        self.cur = self.conn.connection.cursor()
        self.cur.execute("select `dataDI` from `" + Models.TaxaDI2.__tablename__ + "`")
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
            np.transpose([bdays, np.ones(len(bdays))]),
            columns=["dataDI", "indiceDI"],
        )

        self.ftp = FTP('ftp.cetip.com.br')
        self.ftp.login()
        self.ftp.cwd('IndiceDI')
        self.cetip_bdays = self.ftp.nlst()

    def start_requests(self):
        for index, row in self.cetip_dates.iterrows():
            filename = row["dataDI"].replace("-", "") + ".txt"
            url = "ftp://ftp.cetip.com.br/MediaCDI/" + filename
            if (row["dataDI"].replace("-", "") + ".txt") in self.cetip_bdays:
                lines = []
                self.ftp.retrlines("RETR " + filename, lines.append)
                yield Request(url=url, method="FTP", body=lines[0])
            else:
                yield Request(url=url, method="FTP", body="404")
                

    def parse(self, response):
        if response.status == 404:
            url = response.url
            item["msg"] = "Not found at CETIP FTP"
        else:
            url = response.url
            indiceDI = int(response.body.decode())
            dataDI = (
                url[-12:][:4]
                + "-"
                + url[-12:][4:6]
                + "-"
                + url[-12:][6:8]
            )
            item = CetipDIItem()
            item["indiceDI"] = str(indiceDI)
            item["dataDI"] = dataDI

            sql = (
                "INSERT INTO `"
                + Models.TaxaDI2.__tablename__
                + "` (`dataDI`, `indiceDI`) VALUES (?, ?);"
            )
            self.cur.execute(
                sql, (item["dataDI"], item["indiceDI"])
            )
            self.conn.commit()
        yield item