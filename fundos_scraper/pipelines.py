# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os
import zipfile

import MySQLdb
import pandas as pd
import sqlalchemy as sa
from scrapy.exceptions import DropItem
# useful for handling different item types with a single interface
from scrapy.pipelines.files import FilesPipeline
from sqlalchemy import create_engine

import parameters


class FundosScraperPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        return file_name

    def item_completed(self, results, item, info):
        file_paths = [x['path'] for ok, x in results if ok]
        if not file_paths:
            raise DropItem("Item contains no files")
        item['file_paths'] = file_paths
        # let's get the absolute path from the file
        absolute_path = os.path.join(self.store.basedir, file_paths[0])
        zf = zipfile.ZipFile(absolute_path)
        arquivo = zf.open(zf.filelist[0].filename)
        df = pd.read_csv(arquivo, sep=';', engine='python')
        df = df.drop(columns=['TP_FUNDO', 'VL_PATRIM_LIQ'])
        df['DT_COMPTC'] = pd.to_datetime(df.DT_COMPTC)

        engine = create_engine(
            'mysql://' + parameters.user + ':' + parameters.password + '@' + parameters.host + '/' + parameters.database,
        )

        object_columns = [c for c in df.columns[df.dtypes == 'object'].tolist()]
        dtyp = {c: sa.types.VARCHAR(df[c].str.len().max()) for c in object_columns}
        try:
            df.to_sql(parameters.quotes_table_name, dtype=dtyp, con=engine, index=False,
                      if_exists='append', method='multi', chunksize=2000)  # Replace Table_name with your sql table name
        except:
            pass
        engine.execute("REPLACE INTO `" + parameters.scrapy_quotes_table_name +
                       "` (`link`, `ultima_atualizacao`) VALUES ('" + file_paths[0] + "','" + item[
                           'data_atualizacao'] + "')")

        engine.dispose()
        arquivo.close()
        zf.close()
        return item
