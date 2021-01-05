from ExcelParser.MakeDF import CleanDFByConfig
from io import BytesIO
import os
import traceback


def process_sheet(sheet_name, in_mem_file, cdf, filename):
    df_sow, status = None, None
    try:
        df_sow = cdf.get_df(None, bytesIO=in_mem_file, sheet_name=sheet_name, excel_table=True)
    except Exception as e:
        traceback.print_exc()
        df_sow, status =  {
                   "status": "Error",
                   "filename": filename,
                   "error": e,
                   "sheet": sheet_name
               }, None
    else:
        status = {
                   "status": "Success",
                   "filename": filename,
                   "error": "",
                   "sheet": sheet_name
               }
    return {
        f"{sheet_name}":{
            "status":status,
            "df":df_sow
        }
    }


def process_sow(config_path, data_path):
    filename = data_path[data_path.rfind("\\") + 1:]
    cdf = CleanDFByConfig(config_path)
    a = {}
    with open(data_path, "rb") as f:
        in_mem_file = BytesIO(f.read())
        a["sow desc"] = process_sheet("sow desc", in_mem_file, cdf, filename)
        a["sow tactical"] = process_sheet("sow tactical", in_mem_file, cdf, filename)
    return a
