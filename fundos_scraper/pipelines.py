# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import os
import sys
import zipfile

import mysql.connector
import pandas as pd
from pymysql.converters import escape_string
from scrapy.exceptions import DropItem
# useful for handling different item types with a single interface
from scrapy.pipelines.files import FilesPipeline
from sqlalchemy import create_engine
from contextlib import contextmanager
import sys, os
import parameters

@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


class FundosScraperPipeline(FilesPipeline):

    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        return file_name

    def item_completed(self, results, item, info):
        file_paths = [x['path'] for ok, x in results if ok]
        if not file_paths:
            raise DropItem("Item contains no files")
        item['file_paths'] = file_paths
        if item['pipeline'] == 'meses':
            # let's get the absolute path from the file
            absolute_path = os.path.join(self.store.basedir, file_paths[0])
            zf = zipfile.ZipFile(absolute_path)
            arquivo = zf.open(zf.filelist[0].filename)
            df = pd.read_csv(arquivo, sep=';', engine='python')
            df = df.drop(columns=['TP_FUNDO', 'VL_PATRIM_LIQ'])
            df['DT_COMPTC'] = pd.to_datetime(df.DT_COMPTC)

            conn = mysql.connector.connect(
                host=parameters.host,
                user=parameters.user,
                password=parameters.password,
                database=parameters.database
            )
            # Create cursor, used to execute commands
            cursor = conn.cursor()
            sql_insert = "INSERT INTO `" + parameters.quotes_table_name + "`" + \
                         "(`CNPJ_FUNDO`, `DT_COMPTC`, `VL_QUOTA`) " + \
                         "VALUES "
            logging.info("Starting upload to database of " + file_paths[0])
            sql_insert_values = ""
            for row in df.itertuples():
                with suppress_stdout():
                    sql_insert_values += " ('" + row.CNPJ_FUNDO + "','" + row.DT_COMPTC.strftime('%Y-%m-%d') + "','" + str(
                        row.VL_QUOTA) + "','" + str(row.CAPTC_DIA) + "'),"
                if len(sql_insert_values) > 102400 * 5:
                    sql_insert_values = sql_insert_values[:-1] + \
                                        'ON DUPLICATE KEY UPDATE VL_QUOTA = VALUES(' \
                                        'VL_QUOTA) '
                    try:
                        cursor.execute(sql_insert + sql_insert_values + ';')
                    except:
                        logging.error("Failed to upload: " + sql_insert + sql_insert_values)
                    conn.commit()
                    sql_insert_values = ''
                    logging.info(
                        "Uploading rows of " + file_paths[0] + " until " + str(row.Index) + " of " + str(len(df.index)))
            if len(sql_insert_values) > 0:
                sql_insert_values = sql_insert_values[:-1] + \
                                    ' ON DUPLICATE KEY UPDATE VL_QUOTA = VALUES(VL_QUOTA)'
                cursor.execute(sql_insert + sql_insert_values + ';')
                conn.commit()

            cursor.execute("REPLACE INTO `" + parameters.scrapy_quotes_table_name +
                           "` (`link`, `ultima_atualizacao`) VALUES ('" + file_paths[0] + "','" +
                           item[
                               'data_atualizacao'] + "')")
            conn.commit()
            logging.info("Finished upload to database of " + file_paths[0])

            arquivo.close()
            zf.close()
        return item


class FundosScraperPipelineLaminas(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        return file_name

    def item_completed(self, results, item, info):
        if item['pipeline'] == 'laminas':
            file_paths = [x['path'] for ok, x in results if ok]
            if not file_paths:
                raise DropItem("Item contains no files")
            item['file_paths'] = file_paths

            # let's get the absolute path from the file
            absolute_path = os.path.join(self.store.basedir, file_paths[0])
            zf = zipfile.ZipFile(absolute_path)
            arquivo = zf.open(zf.filelist[0].filename)
            df = pd.read_csv(arquivo, sep=';', engine='python', encoding='latin1', quoting=3)
            df['DT_COMPTC'] = pd.to_datetime(df.DT_COMPTC)

            engine = create_engine(
                'mysql://' + parameters.user + ':' + parameters.password + '@' + parameters.host + '/' + parameters.database,
            )

            conn = mysql.connector.connect(
                host=parameters.host,
                user=parameters.user,
                password=parameters.password,
                database=parameters.database
            )
            # Create cursor, used to execute commands
            cursor = conn.cursor()
            sql_insert = "INSERT INTO `" + parameters.description_table_name + "`" + \
                         "(`CNPJ_FUNDO`, `DT_COMPTC`, `DENOM_SOCIAL`, `NM_FANTASIA`)" + \
                         " VALUES "
            logging.info("Starting upload to database of " + file_paths[0])
            sql_insert_values = ""
            for row in df.itertuples():
                sql_insert_values += " ('" + escape_string(row.CNPJ_FUNDO) + "','" \
                                     + row.DT_COMPTC.strftime('%Y-%m-%d') + "','" + escape_string(str(
                    row.DENOM_SOCIAL)) + "','" + escape_string(str(row.NM_FANTASIA)) + "'),"
                if len(sql_insert_values) > 10240:
                    sql_insert_values = sql_insert_values[:-1] + \
                                        ' ON DUPLICATE KEY UPDATE NM_FANTASIA = VALUES(NM_FANTASIA)'

                    try:
                        cursor.execute(sql_insert + sql_insert_values + ';')
                    except:
                        logging.error("Failed to upload: " + sql_insert + sql_insert_values)
                    conn.commit()
                    sql_insert_values = ''
                    logging.info(
                        "Uploading rows of " + file_paths[0] + " until " + str(row.Index) + " of " + str(len(df.index)))
            if len(sql_insert_values) > 0:
                sql_insert_values = sql_insert_values[:-1] + \
                                    ' ON DUPLICATE KEY UPDATE NM_FANTASIA = VALUES(NM_FANTASIA)'
                cursor.execute(sql_insert + sql_insert_values + ';')
                conn.commit()
            cursor.execute(
                "REPLACE INTO `" + parameters.scrapy_description_table_name + "` (`link`, `ultima_atualizacao`) VALUES ('" +
                file_paths[0] + "','" + item['data_atualizacao'] + "')")
            conn.commit()
            logging.info("Finished upload to database of " + file_paths[0])
            engine.dispose()
            arquivo.close()
            zf.close()
        return item


class FundosScraperPipelineTesouroDireto(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        return file_name

    def item_completed(self, results, item, info):
        if item['pipeline'] == 'tesouro':
            file_paths = [x['path'] for ok, x in results if ok]
            if not file_paths:
                raise DropItem("Item contains no files")
            item['file_paths'] = file_paths

            # let's get the absolute path from the file
            absolute_path = os.path.join(self.store.basedir, file_paths[0])
            df = pd.read_csv(absolute_path, sep=';', engine='python', encoding='latin1', quoting=3, decimal=",",
                             dayfirst=True)
            df.columns = [c.replace(' ', '_') for c in df.columns]
            df['Data_Vencimento'] = pd.to_datetime(df.Data_Vencimento, dayfirst=True)
            df['Data_Base'] = pd.to_datetime(df.Data_Base, dayfirst=True)

            engine = create_engine(
                'mysql://' + parameters.user + ':' + parameters.password + '@' + parameters.host + '/' + parameters.database,
            )

            conn = mysql.connector.connect(
                host=parameters.host,
                user=parameters.user,
                password=parameters.password,
                database=parameters.database
            )
            # Create cursor, used to execute commands
            cursor = conn.cursor()
            sql_insert = "INSERT INTO `" + parameters.tesouro_direto_table_name + "` " + \
                         "(`nome`, `vencimento`, `data`, `taxa_compra`, `taxa_venda`, `pu_compra`, `pu_venda`, `pu_base`) VALUES "
            logging.info("Starting upload to database of " + file_paths[0])
            sql_insert_values = ""
            for row in df.itertuples():
                sql_insert_values += " ('" \
                                     + escape_string(row.Tipo_Titulo) + "','" \
                                     + row.Data_Vencimento.strftime('%Y-%m-%d') + "','" \
                                     + row.Data_Base.strftime('%Y-%m-%d') + "','" \
                                     + str(row.Taxa_Compra_Manha) + "','" \
                                     + str(row.Taxa_Venda_Manha) + "','" \
                                     + str(row.PU_Compra_Manha) + "','" \
                                     + str(row.PU_Venda_Manha) + "','" \
                                     + str(row.PU_Base_Manha) + "'),"
                if len(sql_insert_values) > 10240 * 5:
                    sql_insert_values = sql_insert_values[:-1] + \
                                        ' ON DUPLICATE KEY UPDATE taxa_compra = VALUES(taxa_compra), taxa_venda = VALUES(taxa_venda), pu_compra = VALUES(pu_compra), pu_venda = VALUES(pu_venda), pu_base = VALUES(pu_base)'
                    try:
                        cursor.execute(sql_insert + sql_insert_values + ';')
                    except Exception as e:
                        logging.error("Failed to upload: " + str(e))

                    conn.commit()
                    sql_insert_values = ''
                    logging.info(
                        "Uploading rows of " + file_paths[0] + " until " + str(row.Index) + " of " + str(len(df.index)))
            if len(sql_insert_values) > 0:
                sql_insert_values = sql_insert_values[:-1] + \
                                    ' ON DUPLICATE KEY UPDATE taxa_compra = VALUES(taxa_compra), taxa_venda = VALUES(taxa_venda), pu_compra = VALUES(pu_compra), pu_venda = VALUES(pu_venda), pu_base = VALUES(pu_base)'
                cursor.execute(sql_insert + sql_insert_values + ';')
                conn.commit()
            logging.info("Finished upload to database of " + file_paths[0])
            engine.dispose()
        return item
