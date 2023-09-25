import datetime

import sqlalchemy
from sqlalchemy import or_
from sqlalchemy.orm import Session
from sqlalchemy.sql import func, functions

from database.models import CotasFundo, DescricaoFundo, TaxaDI, Tesouro


class DescricaoFundoRepository:
    @staticmethod
    def find_all(db: Session) -> list[DescricaoFundo]:
        return db.query(DescricaoFundo).all()

    @staticmethod
    def save(db: Session, descricaoFundo: DescricaoFundo) -> DescricaoFundo:
        if descricaoFundo.CNPJ_FUNDOj:
            db.merge(descricaoFundo)
        else:
            db.add(descricaoFundo)
        db.commit()
        return descricaoFundo

    @staticmethod
    def exists_by_cnpj(db: Session, cnpj: int) -> bool:
        return db.query(DescricaoFundo).filter(DescricaoFundo.CNPJ_FUNDO == cnpj).first() is not None

    @staticmethod
    def delete_by_cnpj(db: Session, cnpj: int) -> None:
        descricaoFundo = db.query(DescricaoFundo).filter(DescricaoFundo.CNPJ_FUNDO == cnpj).first()
        if descricaoFundo is not None:
            db.delete(descricaoFundo)
            db.commit()

    def find_by_cnpj(db: Session, cnpj: str) -> DescricaoFundo:
        return db.query(DescricaoFundo).filter(DescricaoFundo.CNPJ_FUNDO == cnpj).first()

    def find_by_name(db: Session, name: str) -> DescricaoFundo:
        return db.query(DescricaoFundo).filter(or_(DescricaoFundo.NM_FANTASIA.like('%'+name+'%'), DescricaoFundo.DENOM_SOCIAL.like('%'+name+'%'))).all()

class CotasFundoRepository:
    @staticmethod
    def find_all(db: Session) -> list[CotasFundo]:
        return db.query(CotasFundo).limit(30).all()

    @staticmethod
    def save(db: Session, cotasFundo: CotasFundo) -> CotasFundo:
        if cotasFundo.id:
            db.merge(cotasFundo)
        else:
            db.add(cotasFundo)
        db.commit()
        return cotasFundo

    @staticmethod
    def find_by_id(db: Session, id: int) -> CotasFundo:
        return db.query(CotasFundo).filter(CotasFundo.id == id).first()

    @staticmethod
    def exists_by_id(db: Session, id: int) -> bool:
        return db.query(CotasFundo).filter(CotasFundo.id == id).first() is not None

    @staticmethod
    def delete_by_id(db: Session, id: int) -> None:
        cotasFundo = db.query(CotasFundo).filter(CotasFundo.id == id).first()
        if cotasFundo is not None:
            db.delete(cotasFundo)
            db.commit()

    def find_by_cnpj(db: Session, cnpj: str, data_de=None, data_ate=None) -> list[CotasFundo]:
        if data_de is not None and data_ate is not None:
            fundos = db.query(CotasFundo).filter(CotasFundo.CNPJ_FUNDO == cnpj).filter(
                CotasFundo.DT_COMPTC >= data_de).filter(CotasFundo.DT_COMPTC <= data_ate).all()
        elif data_de is not None:
            fundos = db.query(CotasFundo).filter(CotasFundo.CNPJ_FUNDO == cnpj).filter(
                CotasFundo.DT_COMPTC >= data_de).all()
        else:
            fundos = db.query(CotasFundo).filter(CotasFundo.CNPJ_FUNDO == cnpj).all()
        return fundos


class TaxaDIRepository:
    @staticmethod
    def find_all(db: Session) -> list[TaxaDI]:
        return db.query(TaxaDI).limit(30).all()

    @staticmethod
    def save(db: Session, taxaDI: TaxaDI) -> TaxaDI:
        if taxaDI.id:
            db.merge(taxaDI)
        else:
            db.add(taxaDI)
        db.commit()
        return taxaDI

    @staticmethod
    def find_by_id(db: Session, id: int) -> TaxaDI:
        return db.query(TaxaDI).filter(TaxaDI.id == id).first()

    @staticmethod
    def exists_by_id(db: Session, id: int) -> bool:
        return db.query(TaxaDI).filter(TaxaDI.id == id).first() is not None

    @staticmethod
    def delete_by_id(db: Session, id: int) -> None:
        taxaDI = db.query(TaxaDI).filter(TaxaDI.id == id).first()
        if taxaDI is not None:
            db.delete(taxaDI)
            db.commit()

    def get_taxa_di(db: Session, date_since=None, date_until=None) -> TaxaDI:
        query = db.query(TaxaDI.id,
                         func.max(TaxaDI.dataDI).label("dataDI"),
                         TaxaDI.taxaDIAnual,
                         TaxaDI.taxaDIDiaria,
                         func.exp(
                             functions.sum(
                                 func.log(TaxaDI.taxaDIDiaria)
                             )
                         ).label("taxaDIAcumulada"))
        if date_since is not None and date_until is not None:
            taxas = query.filter(TaxaDI.dataDI >= date_since).filter(TaxaDI.dataDI <= date_until).first()
        elif date_since is not None:
            taxas = query.filter(TaxaDI.dataDI >= date_since).first()
        else:
            taxas = query.first()
        return taxas


class TesouroRepository:
    @staticmethod
    def find_all(db: Session) -> list[Tesouro]:
        return db.query(Tesouro).limit(30).all()

    @staticmethod
    def save(db: Session, tesouro: Tesouro) -> Tesouro:
        if tesouro.id:
            db.merge(tesouro)
        else:
            db.add(tesouro)
        db.commit()
        return tesouro

    @staticmethod
    def find_by_id(db: Session, id: int) -> Tesouro:
        return db.query(Tesouro).filter(Tesouro.id == id).first()

    @staticmethod
    def exists_by_id(db: Session, id: int) -> bool:
        return db.query(Tesouro).filter(Tesouro.id == id).first() is not None

    @staticmethod
    def delete_by_id(db: Session, id: int) -> None:
        tesouro = db.query(Tesouro).filter(Tesouro.id == id).first()
        if tesouro is not None:
            db.delete(tesouro)
            db.commit()

    def get_titulo(db: Session, nome: str, vencimento: str, date_since=None, date_until=None) -> [Tesouro]:
        query = db.query(Tesouro).filter(Tesouro.nome == nome).filter(Tesouro.vencimento == vencimento)
        if date_since is not None and date_until is not None:
            taxas = query.filter(Tesouro.data >= date_since).filter(Tesouro.data <= date_until).all()
        elif date_since is not None:
            taxas = query.filter(Tesouro.data >= date_since).all()
        else:
            taxas = query.all()
        return taxas

    def get_titulos_disponiveis(db: Session) -> [Tesouro]:
        subquery = db.query(Tesouro.nome, Tesouro.vencimento, func.max(Tesouro.data).label('ultima_atualizacao')).group_by(Tesouro.nome, Tesouro.vencimento).subquery()
        query = db.query(Tesouro).join(subquery, sqlalchemy.and_(Tesouro.nome == subquery.c.nome, Tesouro.vencimento == subquery.c.vencimento, Tesouro.data == subquery.c.ultima_atualizacao))
        query.order_by(Tesouro.nome, Tesouro.vencimento)
        return query.all()
