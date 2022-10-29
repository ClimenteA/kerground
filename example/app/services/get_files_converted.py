from app.repository import Repository


def get_files_converted():
    return Repository.get_files()
