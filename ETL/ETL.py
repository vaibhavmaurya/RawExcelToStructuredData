from ExcelParser.MakeDF import CleanDFByConfig
from io import BytesIO
import os
import traceback


def process_sheet(data_path, sheet_name, in_mem_file, cdf, filename):
    df_sow, status, actual_sheet_name = None, None, None
    try:
        df_sow = cdf.get_df(data_path, bytesIO=in_mem_file, sheet_name=sheet_name, excel_table=True)
        actual_sheet_name = df_sow.iloc[0].SheetName or sheet_name
    except Exception as e:
        traceback.print_exc()
        df_sow, status =  {
                   "status": "Error",
                   "filename": filename,
                   "error": e,
                   "sheet": actual_sheet_name
               }, None
    else:
        status = {
                   "status": "Success",
                   "filename": filename,
                   "error": "",
                   "sheet": actual_sheet_name
               }
    return {
            "status":status,
            "df":df_sow
    }


def process_sow(config_path, data_path):
    filename = data_path[data_path.rfind("\\") + 1:]
    cdf = CleanDFByConfig(config_path)
    a = {}
    with open(data_path, "rb") as f:
        in_mem_file = BytesIO(f.read())
        a["sow desc"] = process_sheet(data_path, "sow desc", in_mem_file, cdf, filename)
        a["sow tactical"] = process_sheet(data_path, "sow tactical", in_mem_file, cdf, filename)
    return a
