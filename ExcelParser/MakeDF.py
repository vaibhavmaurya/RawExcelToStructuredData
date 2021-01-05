import pandas as pd
from dateutil.parser import parse
import traceback
import re

from ExcelParser.ExcelParser import BuildDataFrame


class TableNotFoundForLookupPatern(Exception):
    pass


def clean_str(field_series, impurities, *args, **kwargs):
    def clean_string(x):
        if x is None or x is 'None':
            return ''
        elif x in impurities:
            return ''
        else:
            return x

    if field_series.dtype == "O":
        return field_series.apply(clean_string)
    else:
        return field_series


## TODO: Remember you have to extract year from current
def clean_date(field_series, *args, **kwargs):
    origin_date = parse("1900-01-01")

    def parse_to_date(x):
        d = str(x).strip()
        try:
            if d.isnumeric():
                return origin_date + pd.DateOffset(days=int(d))
            elif d == 'None':
                return parse("1900-01-01")
            else:
                k = parse(d)
                return k if len(d) >= 5 else parse(f"2020-{d}-01")
        except:
            return parse("1900-01-01")

    return pd.to_datetime(field_series.apply(parse_to_date), errors="coerce")


def clean_number(field_series, *args, **kwargs):
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    if field_series.dtype == "O":
        return pd.to_numeric(field_series.apply(lambda x: str(x).strip() if is_number(str(x).strip()) else "0"),
                             errors="coerce")
    else:
        return field_series


cleaning_agenda = [("string", clean_str), ("date", clean_date), ("number", clean_number)]


################ Clean df by excel config ####################

class CleanDFByConfig:

    def __init__(self, config_excel_path):
        self.df_config = pd.read_excel(config_excel_path)

    def _getdfconfig_by_sheet(self, sheetname=None):
        return self.df_config[self.df_config["Sheet Name"].apply(
            lambda x: sheetname in str(x).split(","))] if sheetname is not None else self.df_config

    def get_sheets(self, sep=","):
        sheets_array = self.df_config["Sheet Name"].unique()
        a = []
        for x in sheets_array:
            a += str(x).split(sep)
        return list(set(a))

    def get_empty_df(self, sheetname, with_drived_col=False):
        df = self.get_df_by_sheet(sheetname, with_drived_col)
        columns = list(df["To Column"].values)
        columns += ["filename", "ID", "row_number", "SheetName"]
        return pd.DataFrame(columns=columns), columns

    def get_df_by_sheet(self, sheetname=None, with_drived_col=False, align_type="Table"):
        df_sheet = self._getdfconfig_by_sheet(sheetname)
        df_sheet = df_sheet[df_sheet["Derived Column"] != "X"] if not with_drived_col else df_sheet
        df_sheet = df_sheet[df_sheet.AlignType == align_type]
        return df_sheet

    def get_lookup_from_columns(self, sheetname, no_of_fields=3):
        df_sheet = self._getdfconfig_by_sheet(sheetname)
        k = df_sheet["From Column"].values
        no_of_fields = len(k) if len(k) < no_of_fields else no_of_fields
        return "|".join(k[:no_of_fields])

    def clean_df_field_type(self, df, sheetname):
        df_sheet = self._getdfconfig_by_sheet(sheetname)
        cleaning_agenda = [("string", clean_str), ("date", clean_date), ("number", clean_number)]
        for field_type, field_parser in cleaning_agenda:
            l = df_sheet[df_sheet.Type == field_type]
            cols = l[l["Derived Column"] != "X"]["To Column"].values
            for column in cols:
                if column in df.columns:
                    df[column] = field_parser(df[column], [])
        return df

    def get_calculated_columns(self, df, sheetname=None):
        df_sheet = self._getdfconfig_by_sheet(sheetname)
        l = df_sheet[df_sheet["Derived Column"] == "X"]
        for _, row in l.iterrows():
            expression = "{} = {}".format(row["To Column"], row["From Column"])
            # print(expression)
            df.eval(expression, inplace=True)
        return df

    def get_form_data(self, sheetname=None):
        df_config = self.get_df_by_sheet(align_type="Table")
        pass

    def do_not_null(self, df, sheetname=None):
        df_sheet = self._getdfconfig_by_sheet(sheetname)
        l = df_sheet[df_sheet["Not Empty"] == "X"]
        for _, row in l.iterrows():
            if row["To Column"] in df.keys():
                df = df[df[row["To Column"]].isnull() == False]
        return df

    def _get_form_data(self, bd, sheet_name=None):
        df_config = self.get_df_by_sheet(sheet_name, align_type="Form")
        if df_config.shape[0] == 0:
            return None
        column_mappings = {row["From Column"]: row["To Column"] for i, row in
                           df_config.iterrows()}

        form_data = bd.search_regions_form_values(set(column_mappings.keys()), search_stop_at=3)
        return {v: form_data.get(k, '') for k, v in column_mappings.items()}

    ## This is a special case method
    def get_multi_header_table(self, df, bd, row_numbers, columns_pattern, search_patterns):

        strategy = {}
        deliverable_is_empty = False
        lookup_pattern = None

        search_patterns = []
        l, p = [], []
        ## get some 5 project names and deliverables
        search_count = 0
        for _, r in df.iterrows():
            search_patterns.append("|".join([r["Project Name"], r["Deliverable"], r["Deliverable Type"]]).lower())
            if search_count < 4:
                l.append(r["project_name"].lower())
                p.append((r["deliverable"] + "|" + r["deliverable_type"]).lower())
            search_count += 1
        column_patterns = {
            "project_name": l,
            "deliverable": p
        }
        try:
            lookup_pattern = set(columns_pattern["project_name"])
            anchor, _, _ = bd._find_region_by_mode(lookup_pattern, match_threshold=2)
            print("ANCHOR\n")
            print(anchor)
        except Exception as e:
            raise TableNotFoundForLookupPatern(f"Multiheader table extraction failed because: {lookup_pattern}")

            ## row and column of the begining of column
        strategy["project_name"] = anchor["coordinate"]
        # deliverable

        x, y = strategy["project_name"]

        # print(f"Strategy is {strategy}")
        # print(columns_pattern)
        # print("\n")
        # print(search_patterns)
        # print("\n")
        # print(last_level_col)

        found_cols = bd._sheet.scan_sheet(search_patterns, min_row=x + 1, max_row=x + 3, min_col=y, partial_match=True)
        #     print("\n")
        #     print(found_cols)
        ## collect all columns
        y_coords = [val["coordinate"][1] for _, val in found_cols.items() if val is not None]

        if len(y_coords) == 0:
            ## We have reached here since project name has been found but not the deliverables
            deliverable_is_empty = True
            found_cols = bd._sheet.scan_sheet(search_patterns, min_row=x, max_row=x, min_col=y, partial_match=True)
            y_coords = [val["coordinate"][1] for _, val in found_cols.items() if val is not None]

            if len(y_coords) == 0:
                raise TableNotFoundForLookupPatern(f'''Multiheader table extraction failed due to deliverables 
                                                    are not found for the search pattern:\n
                                                    {search_patterns}''')

        if not deliverable_is_empty:
            strategy["deliverable"] = min([val["coordinate"][0] for _, val in found_cols.items() if val is not None]), y

        y_coords.sort()
        print(y_coords)
        dynamic_data = bd._sheet[row_numbers, y_coords]

        extracted_items = {k: bd._sheet[v[0], y_coords] for k, v in strategy.items()}
        if deliverable_is_empty:
            extracted_items["deliverable"] = [''] * len(y_coords)
        # print(extracted_items)
        # make an index
        # print(extracted_items)

        # The moment of truth
        x, y = dynamic_data.shape
        final_data = []
        for i in range(x):
            g = []
            for j in range(y):
                m = {
                    "ID": bd.ID,
                    "row_number": row_numbers[i],
                    "hours": dynamic_data[i, j]
                }

                ## Extract columns header as row
                for k, v in extracted_items.items():
                    m[k] = v[j]
                g.append(m)
            final_data += g

        return pd.DataFrame(final_data)

    # TODO: Bring Empty column to the data based on the type

    def get_df(self, file_path, bytesIO=None, sheet_name=None, excel_table=False, extension=None):
        bd = BuildDataFrame(file_path, bytesIO)
        bd.set_sheet(pattern=[sheet_name])
        try:
            column_mappings = {row["From Column"]: row["To Column"] for i, row in
                               self.get_df_by_sheet(sheet_name, align_type="Table").iterrows()}

            # print(column_mappings)
            # lookup_pattern, column_mappings=None, value_mappings=None,
            #                    partial_match=True, excel_table=False
            df, columns = bd.find_table(self.get_lookup_from_columns(sheet_name), column_mappings=column_mappings,
                                        value_mappings=None, partial_match=True,
                                        excel_table=excel_table)

            form_data = self._get_form_data(bd, sheet_name)
            if form_data is not None:
                m = {}
                for k, v in form_data.items():
                    m[k] = [v] * df.shape[0]
                df = df.assign(**m)

            # sheet_name is used for partial match
            # bd.sheetname is actual sheet name
            df = self.clean_df_field_type(df, sheet_name)
            df = self.get_calculated_columns(df, sheet_name)
            df = self.do_not_null(df, sheet_name)
            df = df.assign(SheetName=[bd.sheetname] * df.shape[0])

            if callable(extension):
                df_extended = extension(df)
            return df
        except Exception as e:
            traceback.print_exc()
            raise e
        finally:
            pass
            bd._wb.close_workbook()