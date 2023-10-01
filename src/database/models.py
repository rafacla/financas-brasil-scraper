import datetime

from sqlalchemy import Column, Date, Float, Integer, String, UniqueConstraint

from src.database.database import Base


class DescricaoFundo(Base):
    __tablename__ = 'fundos_cvm_descricao'

    CNPJ_FUNDO: int = Column(Integer, nullable=False, primary_key=True, index=True)
    DT_COMPTC: datetime.date = Column(Date, nullable=False)
    DENOM_SOCIAL: str = Column(String(150), nullable=False)
    NM_FANTASIA: str = Column(String(150), nullable=True)


class CotasFundo(Base):
    __tablename__ = 'fundos_cvm_cotas'
    __table_args__ = (UniqueConstraint("CNPJ_FUNDO", "DT_COMPTC", name="_CNPJ_FUNDO_DT_COMPTC_UC"), )

    id: int = Column(Integer, primary_key=True)
    CNPJ_FUNDO: int = Column(Integer, nullable=False, unique=False, index=True)
    DT_COMPTC: datetime.date = Column(Date, nullable=False)
    VL_QUOTA: float = Column(Float, nullable=False)


class TaxaDI(Base):
    __tablename__ = 'di_cetip'

    id: int = Column(Integer, primary_key=True)
    dataDI: datetime.date = Column(Date, nullable=False, unique=True)
    taxaDIAnual: float = Column(Float, nullable=False)
    taxaDIDiaria: float = Column(Float, nullable=False)


class Tesouro(Base):
    __tablename__ = 'tesouro_direto'

    id: int = Column(Integer, primary_key=True)
    nome: str = Column(String(18), nullable=False)
    vencimento: datetime.date = Column(Date, nullable=False)
    data: datetime.date = Column(Date, nullable=False)
    taxa_compra: float = Column(Float, nullable=False)
    taxa_venda: float = Column(Float, nullable=False)
    pu_compra: float = Column(Float, nullable=False)
    pu_venda: float = Column(Float, nullable=False)
    pu_base: float = Column(Float, nullable=False)

class Scrapy_Fundos_Cotas(Base):
    __tablename__ = 'scrapy_fundos_cvm_cotas'

    id: int = Column(Integer, primary_key=True)
    link: str = Column(String(50), nullable=False, unique=True)
    ultima_atualizacao: datetime.date = Column(Date, nullable=False)

class Scrapy_Fundos_Descricao(Base):
    __tablename__ = 'scrapy_fundos_cvm_descricao'

    id: int = Column(Integer, primary_key=True)
    link: str = Column(String(50), nullable=False, unique=True)
    ultima_atualizacao: datetime.date = Column(Date, nullable=False)

class Scrapy_Tesouro_Direto(Base):
    __tablename__ = 'scrapy_tesouro_direto'

    id: int = Column(Integer, primary_key=True)
    link: str = Column(String(50), nullable=False, unique=True)
    ultima_atualizacao: datetime.date = Column(Date, nullable=False)