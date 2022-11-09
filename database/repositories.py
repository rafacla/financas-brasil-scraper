from sqlalchemy.orm import Session

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
