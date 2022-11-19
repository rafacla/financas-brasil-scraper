import datetime

from pydantic import BaseModel


class DescricaoFundoBase(BaseModel):
    CNPJ_FUNDO: str
    DT_COMPTC: datetime.date
    DENOM_SOCIAL: str
    NM_FANTASIA: str


class DescricaoFundoRequest(DescricaoFundoBase):
    ...


class DescricaoFundoResponse(DescricaoFundoBase):
    id: int

    class Config:
        orm_mode = True


class CotasFundoBase(BaseModel):
    CNPJ_FUNDO: str
    DT_COMPTC: datetime.date
    VL_TOTAL: float
    VL_QUOTA: float
    CAPTC_DIA: float
    RESG_DIA: float
    NR_COTST: int


class CotasFundoRequest(CotasFundoBase):
    ...


class CotasFundoResponse(CotasFundoBase):
    id: int

    class Config:
        orm_mode = True


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
