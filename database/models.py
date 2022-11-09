from sqlalchemy import Column, Integer, String, Date

import parameters
from database.database import Base


class DescricaoFundo(Base):
    __tablename__ = parameters.description_table_name

    id: int = Column(Integer, primary_key=True, index=True)
    CNPJ_FUNDO: str = Column(String(18), nullable=False, unique=True)
    DT_COMPTC: str = Column(Date, nullable=False)
    DENOM_SOCIAL: int = Column(String(150), nullable=False)
    NM_FANTASIA: int = Column(String(150), nullable=False)
