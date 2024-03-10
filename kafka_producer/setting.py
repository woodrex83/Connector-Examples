import tomllib

from loguru import logger
from pydantic import BaseModel, FilePath
from pydantic_settings import BaseSettings
from typing import Optional


class KafkaSettings(BaseModel):
    topic: str
    bootstrap_server: str
    security_protocol: str = "plaintext"
    ssl_cafile: Optional[FilePath] = ""
    ssl_certfile: Optional[FilePath] = ""
    ssl_keyfile: Optional[FilePath] = ""
    ssl_check_hostname: bool = False    # ssl.endpoint.identification.algorithm


class Settings(BaseSettings):
    kafka: KafkaSettings


def load_cfg(
    path: str = "./kafka_producer/config.toml"
) -> Settings:
    try:
        with open(path, "rb") as fb:
            return Settings.model_validate(tomllib.load(fb))
    except FileNotFoundError:
        logger.error("Config file not found")

settings = load_cfg()