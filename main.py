import datetime
import logging

from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session

import uvicorn
from database.database import engine, Base, get_db
from database.models import DescricaoFundo
from database.repositories import DescricaoFundoRepository
from database.schemas import DescricaoFundoRequest, DescricaoFundoResponse
from database.repositories import CotasFundoRepository
from database.schemas import CotasFundoRequest, CotasFundoResponse

Base.metadata.create_all(bind=engine)

# setup loggers
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)

# get root logger
logger = logging.getLogger(__name__)  # the __name__ resolve to "main" since we are at the root of the project.
# This will get the root logger since no logger in the configuration has this name.

app = FastAPI()


@app.post("/fundos", response_model=DescricaoFundoResponse, status_code=status.HTTP_201_CREATED)
def create(request: DescricaoFundoRequest, db: Session = Depends(get_db)):
    fundo = DescricaoFundoRepository.save(db, DescricaoFundo(**request.dict()))
    return DescricaoFundoResponse.from_orm(fundo)


@app.get("/fundos", response_model=list[DescricaoFundoResponse])
def find_all(db: Session = Depends(get_db)):
    fundos = DescricaoFundoRepository.find_all(db)
    return [DescricaoFundoResponse.from_orm(fundo) for fundo in fundos]


@app.get("/fundos/{cnpj}", response_model=DescricaoFundoResponse)
def find_by_id(cnpj: str, db: Session = Depends(get_db)):
    cnpj = cnpj.replace('.', '').replace('-', '').replace('/', '')
    cnpj_like = ''
    for letra in cnpj:
        cnpj_like += letra + '_'
    cnpj_like = '%' + cnpj_like + '%'
    fundo = DescricaoFundoRepository.find_by_cnpj(db, cnpj_like)
    logger.debug(cnpj_like)
    if not fundo:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Fundo não encontrado, CNPJ: " + cnpj
        )
    return DescricaoFundoResponse.from_orm(fundo)


@app.delete("/fundos/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_by_id(id: int, db: Session = Depends(get_db)):
    if not DescricaoFundoRepository.exists_by_id(db, id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Fundo não encontrado"
        )
    DescricaoFundoRepository.delete_by_id(db, id)
    return DescricaoFundoResponse(status_code=status.HTTP_204_NO_CONTENT)


@app.put("/fundos/{id}", response_model=DescricaoFundoResponse)
def update(id: int, request: DescricaoFundoRequest, db: Session = Depends(get_db)):
    if not DescricaoFundoRepository.exists_by_id(db, id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Fundo não encontrado"
        )
    fundo = DescricaoFundoRepository.save(db, DescricaoFundo(id=id, **request.dict()))
    return DescricaoFundoResponse.from_orm(fundo)


@app.get("/cotas/{cnpj}", response_model=list[CotasFundoResponse])
@app.get("/cotas/{cnpj}/{data_de}", response_model=list[CotasFundoResponse])
@app.get("/cotas/{cnpj}/{data_de}/{data_ate}", response_model=list[CotasFundoResponse])
def cotas_by_cnpj(cnpj: str, data_de=None, data_ate=None, db: Session = Depends(get_db)):
    cnpj = cnpj.replace('.', '').replace('-', '').replace('/', '')
    cnpj_like = ''
    for letra in cnpj:
        cnpj_like += letra + '_'
    cnpj_like = '%' + cnpj_like + '%'
    fundos = CotasFundoRepository.find_by_cnpj(db, cnpj_like, data_de, data_ate)

    if not fundos:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Cotas não encontradas, CNPJ: " + cnpj
        )
    # return CotasFundoResponse.from_orm(fundos)
    return [CotasFundoResponse.from_orm(fundo) for fundo in fundos]


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
