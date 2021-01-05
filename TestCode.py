config_path = "test\\Config.xlsx"
OUTPUT_PATH = "test\\Processed"
data_path = "test\\data\\2020 Novartis SOW_Capmatinib US _V19.xlsx"
# data_path = "Check\\data"
log_path = "test\\Logs"
from ExcelParser.MakeDF import CleanDFByConfig
from io import BytesIO

df = None

cdf = CleanDFByConfig(config_path)

with open(data_path, "rb") as f:
    in_mem_file = BytesIO(f.read())
    df = cdf.get_df(data_path, bytesIO=in_mem_file, sheet_name='sow desc', excel_table=True)

print(df.shape)