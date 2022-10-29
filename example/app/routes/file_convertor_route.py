from fastapi import APIRouter, UploadFile
from app import services
from typing import List

router = APIRouter(
    tags=["Files Convertor"],
    prefix="/file-convertor",
)


@router.get("")
def get_files_converted():
    return services.get_files_converted()


@router.get("/status")
def check_status(msgid: str):
    return services.check_status(msgid)


@router.post("")
def send_files_for_conversion(files: List[UploadFile]):
    return services.send_files_for_conversion(files)
