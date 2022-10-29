from peewee import *
from config import config


db = SqliteDatabase(config.DB)


class Files(Model):
    filepath = CharField()

    class Meta:
        database = db


db.connect()
db.create_tables([Files])
