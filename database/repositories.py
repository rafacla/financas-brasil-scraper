from sqlalchemy.orm import Session

from database.models import CotasFundo
from database.models import DescricaoFundo


class DescricaoFundoRepository:
    @staticmethod
    def find_all(db: Session) -> list[DescricaoFundo]:
        return db.query(DescricaoFundo).all()

    @staticmethod
    def save(db: Session, descricaoFundo: DescricaoFundo) -> DescricaoFundo:
        if descricaoFundo.id:
            db.merge(descricaoFundo)
        else:
            db.add(descricaoFundo)
        db.commit()
        return descricaoFundo

    @staticmethod
    def find_by_id(db: Session, id: int) -> DescricaoFundo:
        return db.query(DescricaoFundo).filter(DescricaoFundo.id == id).first()

    @staticmethod
    def exists_by_id(db: Session, id: int) -> bool:
        return db.query(DescricaoFundo).filter(DescricaoFundo.id == id).first() is not None

    @staticmethod
    def delete_by_id(db: Session, id: int) -> None:
        descricaoFundo = db.query(DescricaoFundo).filter(DescricaoFundo.id == id).first()
        if descricaoFundo is not None:
            db.delete(descricaoFundo)
            db.commit()

    def find_by_cnpj(db: Session, cnpj: str) -> DescricaoFundo:
        return db.query(DescricaoFundo).filter(DescricaoFundo.CNPJ_FUNDO.like(cnpj)).first()


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
