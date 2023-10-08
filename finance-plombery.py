import subprocess
import parameters

from datetime import datetime
from random import randint

from apscheduler.triggers.interval import IntervalTrigger
from plombery import Trigger, get_logger, register_pipeline, task
from pydantic import BaseModel


class InputParams(BaseModel):
  spidername: str

@task
async def run_spider(params: InputParams):
    """run scrapy spider"""
    # using Plombery logger your logs will be stored
    # and accessible on the web UI
    logger = get_logger()

    logger.info("Running spider: " + params.spidername)
    result = subprocess.run(["scrapy", "crawl", params.spidername])
    logger.info(result)

    return result.returncode

register_pipeline(
    id="cetip_di",
    description="Runs scrapy to retrieve DI rate from CETIP",
    tasks = [run_spider],
    triggers = [
        Trigger(
            id="daily",
            name="Daily",
            description="Run the pipeline every day",
            schedule=IntervalTrigger(days=1, start_date=datetime(datetime.now().year,datetime.now().month, datetime.now().day, parameters.cetip_di_hour, 0)),
            params=InputParams(spidername='cetip_di'),
        ),
    ],
    params=InputParams
)

register_pipeline(
    id="tesouro",
    description="Runs scrapy to retrieve tesouro direto data from gov. site",
    tasks = [run_spider],
    triggers = [
        Trigger(
            id="daily",
            name="Daily",
            description="Run the pipeline every day",
            schedule=IntervalTrigger(days=1, start_date=datetime(datetime.now().year,datetime.now().month, datetime.now().day, parameters.tesour_direto_hour, 0)),
            params=InputParams(spidername="tesouro"),
        ),
    ],
    params=InputParams
)

register_pipeline(
    id="fundos_cvm_laminas",
    description="Runs scrapy to retrieve laminas from fundos_cvm",
    tasks = [run_spider],
    triggers = [
        Trigger(
            id="daily",
            name="Daily",
            description="Run the pipeline every day",
            schedule=IntervalTrigger(days=1, start_date=datetime(datetime.now().year,datetime.now().month, datetime.now().day, parameters.fundos_cvm_laminas_hour, 0)),
            params=InputParams(spidername="fundos_cvm_laminas"),
        ),
    ],
    params=InputParams
)

register_pipeline(
    id="fundos_cvm_cotas",
    description="Runs scrapy to retrieve cotas from fundos_cvm",
    tasks = [run_spider],
    triggers = [
        Trigger(
            id="daily",
            name="Daily",
            description="Run the pipeline every day",
            schedule=IntervalTrigger(days=1, start_date=datetime(datetime.now().year,datetime.now().month, datetime.now().day, parameters.fundos_cvm_laminas_hour, 0)),
            params=InputParams(spidername="fundos_cvm_cotas"),
        ),
    ],
    params=InputParams
)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("plombery:get_app", reload=True, factory=True)