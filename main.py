import os

from Utils.Utils import Utils
from Utils.DfMaker import DfMaker
import pandas as pd
from Utils.SQLMaker import SQLMaker

if __name__ == '__main__':
    ut = Utils()

    ambiti = os.listdir(ut.input_path)
    fogli_corretti = []

    for ambito in ambiti:

        if ambito == "old":
            continue
        ut.ambito = ambito

        for file in os.listdir(ut.join_paths([ut.input_path, ambito])):

            if file[-5:] != ".xlsx":
                continue

            ut.input_name = file[:-5]
            xls = pd.ExcelFile(ut.join_paths([ut.input_path, ambito, file]))

            # da rimuovere, solo per test
            xls_sheet_name = xls.sheet_names

            # LOG = ""

            for sheet_name in xls_sheet_name:

                ut.log("init", [ambito, sheet_name])

                ut.sheet_name = sheet_name
                try:

                    df = pd.read_excel(xls, sheet_name, dtype=object)
                    df = df.dropna(how="all")
                    df = df.where(~ pd.isna(df), None)

                    ut.log("excel", df)
                    table_name = ut.get_table_name(df, sheet_name)
                    dfm = DfMaker(ut=ut, excel_sheet=df)
                    table_info = dfm.get_info()

                    ut.log("df_reader", table_info)
                    ut.log("SQL_writer")
                    sqlm = SQLMaker(ut=ut, table_info=table_info, table_name=table_name)
                    sqlm.create_L0()
                    sqlm.create_L0_SCARTI()
                    sqlm.create_L1()
                    sqlm.view_builder()
                    sqlm.uspMaker.create_usp(table_info, table_name)
                    flusso = "_".join(table_name.split(" "))

                    ut.log("save_procedure")

                    ut.write_sql_query(sqlm.sqlQuery + sqlm.uspMaker.sqlQuery, flusso, "wholeProcedure.sql")

                    ut.log("end", [ambito, sheet_name])
                    fogli_corretti.append(sheet_name)

                except Exception as e:
                    print(
                        "Durante l'elaborazione del foglio: \"" + sheet_name + "\" la seguente eccezione è stata lanciata: ")
                    print("\t---> " + str(e))

    print("\n\nFogli correttamente stampati: ")
    for e in fogli_corretti:
        print("\t" + e)
