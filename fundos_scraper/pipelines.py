# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import os
import sys
import zipfile
from contextlib import contextmanager

import pandas as pd
import sqlalchemy
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline
from sqlalchemy import event, Integer
from sqlalchemy.dialects.sqlite import insert

import database.models as Models
from database.database import engine, get_db

import parameters as parameters

@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout

def escape_string(string):
    return str(string).replace("'", "")

class FundosScraperPipeline(FilesPipeline):

    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        return file_name

    def item_completed(self, results, item, info):
        if item.get('pipeline',0) == 'meses':
            file_paths = [x['path'] for ok, x in results if ok]
            if not file_paths:
                raise DropItem("Item contains no files")
            item['file_paths'] = file_paths
            if item['pipeline'] == 'meses':
                # let's get the absolute path from the file
                absolute_path = os.path.join(self.store.basedir, file_paths[0])
                logging.info("Started upload to database of " + file_paths[0])

                zf = zipfile.ZipFile(absolute_path)
                arquivo = zf.open(zf.filelist[0].filename)
                df = pd.read_csv(arquivo, sep=';', engine='python')
                df = df[["CNPJ_FUNDO","DT_COMPTC","VL_QUOTA"]]
                df['DT_COMPTC'] = pd.to_datetime(df.DT_COMPTC)
                df['CNPJ_FUNDO'] = df["CNPJ_FUNDO"].str.replace(r'\W','', regex=True)

                conn = next(get_db()).connection()
                
                def insert_on_conflict_update(table, conn, keys, data_iter):
                    data = [dict(zip(keys, row)) for row in data_iter]
                    stmt = (
                        insert(Models.CotasFundo)
                        .values(data)
                    )
                    stmt = stmt.on_conflict_do_update(index_elements=["CNPJ_FUNDO", "DT_COMPTC"], set_=dict(VL_QUOTA=stmt.excluded.VL_QUOTA))
                    result = conn.execute(stmt)
                    return result.rowcount

                
                @event.listens_for(engine, "before_cursor_execute")
                def receive_before_cursor_execute(conn, 
                cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True            
                df.to_sql(Models.CotasFundo.__tablename__, conn, index=False, if_exists="append", method=insert_on_conflict_update, chunksize=5000, dtype={'CNPJ_FUNDO': Integer})

                cursor = conn.connection.cursor()
                cursor.execute("REPLACE INTO `" + Models.Scrapy_Fundos_Cotas.__tablename__ +
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
        if item.get('pipeline',0) == 'laminas':
            file_paths = [x['path'] for ok, x in results if ok]
            if not file_paths:
                raise DropItem("Item contains no files")
            item['file_paths'] = file_paths

            # let's get the absolute path from the file
            absolute_path = os.path.join(self.store.basedir, file_paths[0])
            logging.info("Started upload to database of " + file_paths[0])

            zf = zipfile.ZipFile(absolute_path)
            arquivo = zf.open(zf.filelist[0].filename)
            df = pd.read_csv(arquivo, sep=';', engine='python', encoding='latin1', quoting=3)
            df = df[["CNPJ_FUNDO","DT_COMPTC","DENOM_SOCIAL","NM_FANTASIA"]]
            df['DT_COMPTC'] = pd.to_datetime(df.DT_COMPTC)
            df['CNPJ_FUNDO'] = df["CNPJ_FUNDO"].str.replace(r'\W','', regex=True)

            conn = next(get_db()).connection()
            
            def insert_on_conflict_update(table, conn, keys, data_iter):
                data = [dict(zip(keys, row)) for row in data_iter]
                stmt = (
                    insert(Models.DescricaoFundo)
                    .values(data)
                )
                stmt = stmt.on_conflict_do_update(index_elements=['CNPJ_FUNDO'], set_=dict(NM_FANTASIA=stmt.excluded.NM_FANTASIA, DENOM_SOCIAL=stmt.excluded.DENOM_SOCIAL, DT_COMPTC=stmt.excluded.DT_COMPTC))
                result = conn.execute(stmt)
                return result.rowcount

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, 
            cursor, statement, params, context, executemany):
                if executemany:
                    cursor.fast_executemany = True
            
            
            df.to_sql(Models.DescricaoFundo.__tablename__, conn, index=False, if_exists="append", method=insert_on_conflict_update, dtype={'CNPJ_FUNDO': Integer})

            cursor = conn.connection.cursor()
            cursor.execute("REPLACE INTO `" + Models.Scrapy_Fundos_Descricao.__tablename__ +
                           "` (`link`, `ultima_atualizacao`) VALUES ('" + file_paths[0] + "','" +
                           item[
                               'data_atualizacao'] + "')")
            conn.commit()
            logging.info("Finished upload to database of " + file_paths[0])


            arquivo.close()
            zf.close()
        return item

class FundosScraperPipelineTesouroDireto(FilesPipeline):
    def file_path(self, request, response=None, info=None):
        file_name: str = request.url.split("/")[-1]
        return file_name

    def item_completed(self, results, item, info):
        if item.get('pipeline',0) == 'tesouro':
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

            # Create cursor, used to execute commands
            conn = next(get_db()).connection().connection.dbapi_connection
            cursor = conn.cursor()

            sql_insert = "REPLACE INTO `" + Models.Tesouro.__tablename__ + "` " + \
                         "(`nome`, `vencimento`, `data`, `taxa_compra`, `taxa_venda`, `pu_compra`, `pu_venda`, `pu_base`) VALUES "
            logging.info("Starting upload to database of " + file_paths[0])
            sql_insert_values = ""
            for row in df.itertuples():
                sql_insert_values += " ('" \
                                     + sqlalchemy.text(row.Tipo_Titulo).text + "','" \
                                     + row.Data_Vencimento.strftime('%Y-%m-%d') + "','" \
                                     + row.Data_Base.strftime('%Y-%m-%d') + "','" \
                                     + str(row.Taxa_Compra_Manha) + "','" \
                                     + str(row.Taxa_Venda_Manha) + "','" \
                                     + str(row.PU_Compra_Manha) + "','" \
                                     + str(row.PU_Venda_Manha) + "','" \
                                     + str(row.PU_Base_Manha) + "'),"
                if len(sql_insert_values) > 10240 * 500:
                    sql_insert_values = sql_insert_values[:-1] 
                    try:
                        cursor.execute(sql_insert + sql_insert_values + ';')
                    except Exception as e:
                        logging.error("Failed to upload: " + str(e))

                    conn.commit()
                    sql_insert_values = ''
                    logging.info(
                        "Uploading rows of " + file_paths[0] + " until " + str(row.Index) + " of " + str(len(df.index)))
            if len(sql_insert_values) > 0:
                sql_insert_values = sql_insert_values[:-1]
                cursor.execute(sql_insert + sql_insert_values + ';')
                conn.commit()
            logging.info("Finished upload to database of " + file_paths[0])
            engine.dispose()
        return item

