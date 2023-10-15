import datetime

from pydantic import BaseModel
from pydantic.schema import Optional


class DescricaoFundoBase(BaseModel):
    CNPJ_FUNDO: Optional[str]
    DT_COMPTC: Optional[datetime.date]
    DENOM_SOCIAL: Optional[str]
    NM_FANTASIA: Optional[str]


class DescricaoFundoRequest(DescricaoFundoBase):
    ...


class DescricaoFundoResponse(DescricaoFundoBase):
    ...

    class Config:
        orm_mode = True
        from_attributes = True


class CotasFundoBase(BaseModel):
    CNPJ_FUNDO: str
    DT_COMPTC: datetime.date
    VL_QUOTA: float


class CotasFundoRequest(CotasFundoBase):
    ...


class CotasFundoResponse(CotasFundoBase):
    id: int

    class Config:
        orm_mode = True
        from_attributes = True


class TaxaDIBase(BaseModel):
    dataDI: datetime.date
    taxaDIAnual: float
    taxaDIDiaria: float


class TaxaDIRequest(TaxaDIBase):
    ...


class TaxaDIResponse(TaxaDIBase):
    id: int
    taxaDIAcumulada: float

    class Config:
        orm_mode = True
        from_attributes = True


class TesouroBase(BaseModel):
    nome: str
    vencimento: datetime.date
    data: datetime.date
    taxa_compra: float
    taxa_venda: float
    pu_compra: float
    pu_venda: float
    pu_base: float


class TesouroRequest(TesouroBase):
    ...


class TesouroResponse(TesouroBase):
    id: int

    class Config:
        orm_mode = True
        from_attributes = True
