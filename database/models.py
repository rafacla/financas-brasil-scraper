import datetime

from sqlalchemy import Column, Integer, String, Date, Float

import parameters
from database.database import Base


class DescricaoFundo(Base):
    __tablename__ = parameters.description_table_name

    id: int = Column(Integer, primary_key=True, index=True)
    CNPJ_FUNDO: str = Column(String(18), nullable=False, unique=True)
    DT_COMPTC: datetime.date = Column(Date, nullable=False)
    DENOM_SOCIAL: str = Column(String(150), nullable=False)
    NM_FANTASIA: str = Column(String(150), nullable=False)


class CotasFundo(Base):
    __tablename__ = parameters.quotes_table_name

    id: int = Column(Integer, primary_key=True, index=True)
    CNPJ_FUNDO: str = Column(String(18), nullable=False, unique=True)
    DT_COMPTC: datetime.date = Column(Date, nullable=False)
    VL_TOTAL: float = Column(Float, nullable=False)
    VL_QUOTA: float = Column(Float, nullable=False)
    CAPTC_DIA: float = Column(Float, nullable=False)
    RESG_DIA: float = Column(Float, nullable=False)
    NR_COTST: int = Column(Integer, nullable=False)