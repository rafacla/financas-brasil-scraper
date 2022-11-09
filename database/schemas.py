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
