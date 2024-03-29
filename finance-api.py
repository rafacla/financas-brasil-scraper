import datetime
import logging

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy.orm import Session

import parameters
from database.database import Base, engine, get_db
from database.models import DescricaoFundo
from database.repositories import (CotasFundoRepository,
                                   DescricaoFundoRepository, TaxaDIRepository,
                                   TesouroRepository)
from database.schemas import (CotasFundoResponse, DescricaoFundoRequest,
                              DescricaoFundoResponse, TaxaDIResponse,
                              TesouroResponse)

Base.metadata.create_all(bind=engine)

logger = logging.getLogger(__name__) 
app = FastAPI()


@app.get("/taxaDI/{date_since}", response_model=TaxaDIResponse)
@app.get("/taxaDI/{date_since}/{date_until}", response_model=TaxaDIResponse)
def get_taxa_di(date_since: datetime.date, date_until=None, db: Session = Depends(get_db)):
    taxa = TaxaDIRepository.get_taxa_di(db, date_since, date_until)
    if not taxa:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Taxa não encontrada, verifique as datas limites"
        )
    return TaxaDIResponse.validate(taxa)


@app.post("/fundos", response_model=DescricaoFundoResponse, status_code=status.HTTP_201_CREATED)
def create(request: DescricaoFundoRequest, db: Session = Depends(get_db)):
    fundo = DescricaoFundoRepository.save(db, DescricaoFundo(**request.dict()))
    return DescricaoFundoResponse.validate(fundo)


@app.get("/fundos", response_model=list[DescricaoFundoResponse])
def find_all(db: Session = Depends(get_db)):
    fundos = DescricaoFundoRepository.find_all(db)
    return [DescricaoFundoResponse.validate(fundo) for fundo in fundos]


@app.get("/fundos/{cnpj}", response_model=DescricaoFundoResponse)
def find_by_id(cnpj: str, db: Session = Depends(get_db)):
    cnpj = cnpj.replace('.', '').replace('-', '').replace('/', '')
    fundo = DescricaoFundoRepository.find_by_cnpj(db, cnpj)
    if not fundo:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Fundo não encontrado, CNPJ: " + cnpj
        )
    return DescricaoFundoResponse.validate(fundo)

@app.get("/fundos/name/{name}", response_model=list[DescricaoFundoResponse])
def find_by_name(name: str, db: Session = Depends(get_db)):
    fundos = DescricaoFundoRepository.find_by_name(db, name)
    if not fundos:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Fundo não encontrado, nome: " + name
        )
    return [DescricaoFundoResponse.validate(fundo) for fundo in fundos]


@app.delete("/fundos/{cnpj}", status_code=status.HTTP_204_NO_CONTENT)
def delete_by_id(cnpj: int, db: Session = Depends(get_db)):
    if not DescricaoFundoRepository.exists_by_cnpj(db, cnpj):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Fundo não encontrado"
        )
    DescricaoFundoRepository.delete_by_cnpj(db, cnpj)
    return DescricaoFundoResponse(status_code=status.HTTP_204_NO_CONTENT)


@app.put("/fundos/{cnpj}", response_model=DescricaoFundoResponse)
def update(cnpj: int, request: DescricaoFundoRequest, db: Session = Depends(get_db)):
    if not DescricaoFundoRepository.exists_by_cnpj(db, cnpj):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Fundo não encontrado"
        )
    fundo = DescricaoFundoRepository.save(db, DescricaoFundo(id=cnpj, **request.dict()))
    return DescricaoFundoResponse.validate(fundo)


@app.get("/cotas/{cnpj}", response_model=list[CotasFundoResponse])
@app.get("/cotas/{cnpj}/{data_de}", response_model=list[CotasFundoResponse])
@app.get("/cotas/{cnpj}/{data_de}/{data_ate}", response_model=list[CotasFundoResponse])
def cotas_by_cnpj(cnpj: str, data_de=None, data_ate=None, db: Session = Depends(get_db)):
    cnpj = cnpj.replace('.', '').replace('-', '').replace('/', '')
    cnpj_like = cnpj

    fundos = CotasFundoRepository.find_by_cnpj(db, cnpj_like, data_de, data_ate)

    if not fundos:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Cotas não encontradas, CNPJ: " + cnpj_like
        )
    # return CotasFundoResponse.validate(fundos)
    return [CotasFundoResponse.validate(fundo) for fundo in fundos]


@app.get("/tesouro/{titulo}/{vencimento}", response_model=list[TesouroResponse])
@app.get("/tesouro/{titulo}/{vencimento}/{data_de}", response_model=list[TesouroResponse])
@app.get("/tesouro/{titulo}/{vencimento}/{data_de}/{data_ate}", response_model=list[TesouroResponse])
def tesouro(titulo: str, vencimento: str, data_de=None, data_ate=None, db: Session = Depends(get_db)):
    fundos = TesouroRepository.get_titulo(db, titulo, vencimento, data_de, data_ate)

    if not fundos:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Título não encontrado: " + titulo
        )
    # return CotasFundoResponse.validate(fundos)
    return [TesouroResponse.validate(fundo) for fundo in fundos]


@app.get("/tesouro/todos", response_model=list[TesouroResponse])
def tesouros_disponiveis(db: Session = Depends(get_db)):
    return TesouroRepository.get_titulos_disponiveis(db)


if __name__ == "__main__":
    uvicorn.run(app, host = parameters.api_host, port = parameters.api_port)