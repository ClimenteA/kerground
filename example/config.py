from dataclasses import dataclass


@dataclass
class Config:
    PORT: int = 3000
    DB: str = "./database.db"


config = Config()
