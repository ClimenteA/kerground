from typing import List
from app.dependencies import Files


class Repository:
    @staticmethod
    def save_files(filenames: List[str]):
        for filename in filenames:
            Files.create(filepath=filename).save()

    @staticmethod
    def get_files():
        filepaths = []
        for item in Files.select():
            filepaths.append(item.filepath)
        return filepaths
