from datetime import date


class UspMaker:
    
    def __init__(self, ut):
        
        self.ut = ut
        self.sqlQuery = ""
        
        
    def get_table_name(self, table_name):
        return "T_" + self.ut.ambito + "_" + table_name

    # dtae = [nome tabella (se serve)].[nome colonna]
    def conv_date(self, date):
        if self.ut.codifica_data == "106":
            return " TRY_CONVERT (date, stuff(stuff(" + date + ", 6, 0, ' '), 3, 0, ' '), "+ self.ut.codifica_data +")"
        elif self.ut.codifica_data == "106 + left":
            return " TRY_CONVERT (date, stuff(stuff(LEFT(" + date + ", 9), 6, 0, ' '), 3, 0, ' '), " + self.ut.codifica_data.split(" ")[0] + ")"
        else:
            return " TRY_CONVERT (date," + date + ", " + self.ut.codifica_data + ")"

    def conv_numeric(self, row, isNull=True):
        str_init = "TRY_CAST("
        if isNull:
            str_end = ") IS  NULL)"
        else:
            str_end = ") "

        if row.Tipo == "int":
            str_med = "[" + row.NomeColonna + "] as int"
        elif row.Tipo == "bit":
            str_med = "[" + row.NomeColonna + "] as bit"
        else:

            if isinstance(row.Lunghezza, int):
                row.Lunghezza = [row.Lunghezza, 0]

            str_med = "REPLACE([" + row.NomeColonna + "], ',', '.') as NUMERIC (" + str(
                int(row.Lunghezza[0]) + int(row.Lunghezza[1])) + "," + str(row.Lunghezza[1]) + ")"
        return str_init + str_med + str_end

    def merge_row(self, row):
        if row.Tipo == "varchar":
            return ",[" + row.NomeColonna + "]"
        elif row.Tipo == "date":
            return "," + self.conv_date("[" + row.NomeColonna + "]")[1:] + "as [" + row.NomeColonna + "]"
        elif row.Tipo in ["bit", "int", "numeric"]:
            return "," + self.conv_numeric(row, False) + "as [" + row.NomeColonna + "]"
        else:
            return ",[" + row.NomeColonna + "]"

    
    def prepare_comment(self, table_name):
        return "/* \n ============================================= \n\nAutore: Giulio Bagnoli\nDescrizione:\n\tProcedura di caricamento dalla tabella [L0].[T_" + self.ut.ambito + "_" + table_name + "] alla tabella [L1].[T_" + self.ut.ambito + "_" + table_name + "].\n\tIl caricamento segue una logica di MERGE (Insert + Update)\nHistory:\n\t" + date.today().strftime(
            "%d/%m/%Y") + ": Data di creazione\nEsempio:\n\texec [L1].[usp_Load_T_" + self.ut.ambito + "_" + table_name + "]\n\t\t@Exec_ID = -2147483541\n\n============================================= \n*/"

    def prepare_signature(self, table_name):
        return "\n\nCREATE\tPROCEDURE [L1].[usp_Load_T_" + self.ut.ambito + "_" + table_name + "]\n\t@Exec_Id [int]\nWITH EXECUTE AS CALLER\nAS\n\tSET LANGUAGE us_english;\n\tSET NOCOUNT ON\n;"

    def prepare_declare(self):
        sql_query = self.ut.concat_strs(1, 1, ["DECLARE"])
        sql_query += self.ut.concat_strs(1, 2, [
            "@ProcName\tvarchar(255) = CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)), N'.', QUOTENAME(OBJECT_NAME("
            "@@PROCID)))", 
            ",@Step\tVARCHAR(500) =''", ",@Now\tdatetime = getdate()", ",@ID_Flusso\tint",
            ",@maxDate\tdate = '99991231' --'12/31/9999'"])
        sql_query += self.ut.concat_strs(1, 1, [";", "", "BEGIN TRY"])
        return sql_query + " \n" * 2

    def get_info_from_table(self, table_name):
        sql_query = self.ut.concat_strs(1, 2,
                                ["SET @Step = '1. Get delle informazioni dalla tabella [JOB].[T_Flusso_DataLoad]'", ";",
                                 "SELECT TOP 1"])
        sql_query += self.ut.concat_strs(1, 3, ["@ID_Flusso = [ID_Flusso]"])
        sql_query += self.ut.concat_strs(1, 2, ["FROM [JOB].[T_Flusso_DataLoad]", "WHERE [Exec_Id] = @Exec_Id", ";", "",
                                        "UPDATE [L0].[T_" + self.ut.ambito + "_" + table_name + "]",
                                        "SET [BitMask_Scarti] = 0 ", ";"])
        return sql_query + " \n" * 2

    def DUPLICATE_KEY_case(self, info, table_name):
        sql_query = self.ut.concat_strs(1, 2, ["SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'", ";",
                                       "UPDATE [sn]",
                                       "SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]",
                                       "FROM [L0].[T_" + self.ut.ambito + "_" + table_name + "] [sn]", "JOIN ( SELECT "])

        if len(info.loc[info["Key"]]) != 0:
            keys = [",[sn].[" + str(name_col) + "]" for name_col in info.loc[info["Key"]].NomeColonna]
            keys[0] = keys[0][1:]
            sql_query += self.ut.concat_strs(1, 3, keys)
        else:
            sql_query += self.ut.concat_strs(1, 3, ["Inserire qui le eventuali chiavi nella forma [sn].[key]"])

        sql_query += self.ut.concat_strs(1, 2,
                                 ["FROM [L0].[T_" + self.ut.ambito + "_" + table_name + "] [sn]", "WHERE [Exec_Id] = @Exec_Id"])

        if len(info.loc[info["Key"]]) != 0:
            keys = ["AND [sn].[" + str(name_col) + "] <> '' AND [sn].[" + str(name_col) + "] IS NOT NULL" for name_col in
                    info.loc[info["Key"]].NomeColonna]
            keys[0] = keys[0][4:]
            sql_query += self.ut.concat_strs(1, 3, keys)
        else:
            sql_query += self.ut.concat_strs(1, 3, ["Inserire qui le eventuali chiavi nella forma [sn].[key] <> '' AND [sn].[key] is not null"])

        sql_query += self.ut.concat_strs(1, 2, ["GROUP BY"])

        if len(info.loc[info["Key"]]) != 0:
            keys = [",[sn].[" + str(name_col) + "]" for name_col in info.loc[info["Key"]].NomeColonna]
            keys[0] = keys[0][1:]
            sql_query += self.ut.concat_strs(1, 3, keys)
        else:
            sql_query += self.ut.concat_strs(1, 3, ["Inserire qui le eventuali chiavi nella forma [sn].[key]"])

        sql_query += self.ut.concat_strs(1, 2, ["HAVING COUNT(*) > 1"])
        sql_query += self.ut.concat_strs(1, 1, [") [sn2]", "on"])

        if len(info.loc[info["Key"]]) != 0:
            keys = ["AND [sn].[" + str(name_col) + "] = [sn2].[" + str(name_col) + "]" for name_col in
                    info.loc[info["Key"]].NomeColonna]
            keys[0] = keys[0][4:]
            sql_query += self.ut.concat_strs(1, 2, keys)
        else:
            sql_query += self.ut.concat_strs(1, 2, ["Inserire qui le eventuali chiavi nella forma [sn].[key] = [sn2].[key"])

        sql_query += self.ut.concat_strs(1, 1, ["CROSS APPLY [L0_SCARTI].[T_Desc_Scarti] scarti", "WHERE [Exec_Id]=@Exec_Id"])
        sql_query += self.ut.concat_strs(1, 2, ["AND [scarti].[Cod_Scarto] = 'DUPLICATE_KEY' --Codice d'errore ",
                                        "AND [scarti].[ID_Flusso] = @ID_Flusso", "AND [scarti].[Flag_Enabled] = 1"])
        return sql_query + " \n" * 2

    def GET_ONE_DUPLICATE_KEY_case(self, info, table_name):
        sql_query = self.ut.concat_strs(1, 1,
                                ["--Viene preso solamente una riga tra le N righe duplicate.",
                                "--Viene presa la prima riga del file (Ordinament o per Row_Id ASC)","SET @Step = "
                                                                                                     "'2.3 Scarti: "
                                                                                                     "Applicazione "
                                                                                                     "criterio di "
                                                                                                     "scarto "
                                                                                                     "GET_ONE_DUPLICATE_KEY'", ";",
                                 "UPDATE [sn]",
                                 "SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]",
                                 "FROM [L0].[T_" + self.ut.ambito + "_" + table_name + "] [sn]", "JOIN ("])
        sql_query += self.ut.concat_strs(1, 2, ["SELECT "])

        if len(info.loc[info["Key"]]) != 0:
            keys = [",[r].[" + str(name_col) + "]" for name_col in info.loc[info["Key"]].NomeColonna]
            keys[0] = keys[0][1:]
        else:
            keys = ["Inserire qui le chiavi nella forma [r].[key]"]
        sql_query += self.ut.concat_strs(1, 3, keys + [",[r].[Row_Id]", ",[r].[Exec_Id]", ",ROW_NUMBER() OVER("])

        sql_query += self.ut.concat_strs(1, 3, ["PARTIOTION BY"])

        if len(info.loc[info["Key"]]) != 0:
            keys = [",[r].[" + str(name_col) + "]" for name_col in info.loc[info["Key"]].NomeColonna]
            keys[0] = keys[0][1:]
            sql_query += self.ut.concat_strs(1, 5, keys)
        else:
            sql_query += self.ut.concat_strs(1, 5, ["Inserire qui le chiavi nella forma [r].[key]"])

        sql_query += self.ut.concat_strs(1, 4, ["ORDER BY"])
        sql_query += self.ut.concat_strs(1, 5, ["[r].[Row_Id] ASC", ",[r].[Exec_Id] ASC]"])

        sql_query += self.ut.concat_strs(1, 2, [") as [rn]", "FROM [L0].[" + self.get_table_name(table_name) + "] [r]",
                                        "WHERE [Exec_Id] = @Exec_Id"])
        sql_query += self.ut.concat_strs(1, 1, [") [sn2]"])
        sql_query += self.ut.concat_strs(1, 2, ["on"])

        if len(info.loc[info["Key"]]) != 0:
            keys = ["AND [sn].[" + str(name_col) + "] = [sn2].[" + str(name_col) + "]" for name_col in
                    info.loc[info["Key"]].NomeColonna]
            keys[0] = keys[0][4:]
        else:
            keys = ["Inserire qui le chiavi nella forma: AND [sn].[key] = [sn2].[key]"]
        sql_query += self.ut.concat_strs(1, 3,
                                 keys + ["AND [sn].[Row_Id] = [sn2].[Row_Id]", "AND [sn].[Exec_Id] = [sn2].[Exec_Id]"])

        sql_query += self.ut.concat_strs(1, 1, keys + ["CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]",
                                               "WHERE [sn].[Exec_Id] = @Exec_Id"])

        sql_query += self.ut.concat_strs(1, 2, ["AND [scarti].[Cod_Scarto] = 'GET_ONE_DUPLICATE_KEY' --Codice d'errore",
                                        "AND [scarti].[ID_Flusso] = @ID_Flusso", "AND [scarti].[Flag_Enabled] = 1",
                                        "AND [sn2].[rn] > 1"])
        sql_query += self.ut.concat_strs(1, 1, [";"])

        return sql_query + " \n" * 2

    def GET_EMPTY_KEY_case(self, info, table_name):
        sql_query = self.ut.concat_strs(1, 1, [
                                       "SET @Step = '2.2 Scarti: Applicazione criterio di scarto EMPTY_KEY'",
                                       ";", "UPDATE [sn]",
                                       "SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]",
                                       "FROM [L0].[" + self.get_table_name(table_name) + "] [sn]",
                                        "CROSS JOIN[L0_SCARTI].[T_Desc_Scarti][scarti]",
                                        "where[Exec_Id] = @Exec_Id"])
        sql_query += self.ut.concat_strs(1, 2, ["AND [scarti].[Cod_Scarto] = 'EMPTY_KEY' --Codice d'errore",
                                                "AND [scarti].[ID_Flusso] = @ID_Flusso",
                                                "AND [scarti].[Flag_Enabled] = 1",
                                                "AND ("])

        if len(info.loc[info["Key"]]) != 0:
            keys = ["OR ([sn].[" + str(name_col) + "] is null OR [sn].[" + str(name_col) + "] = '')" for name_col in info.loc[info["Key"]].NomeColonna]
            keys[0] = keys[0][3:]
        else:
            keys = ["Inserire qui le chiavi nella forma OR ([sn].[key] is null OR [sn].[key] = '')"]
        sql_query += self.ut.concat_strs(1, 3, keys)
        sql_query += self.ut.concat_strs(1, 2, [")"])
        sql_query += self.ut.concat_strs(1, 1, [";"])


        return sql_query + " \n" * 2

    def EMPTY_DATE_case(self, info, table_name):
        sql_query = self.ut.concat_strs(1, 1, ["--Esclusione Date Null",
                                       "SET @Step = '2.4 Scarti: Applicazione criterio di scarto EMPTY_DATE'", ";",
                                       "UPDATE [sn]",
                                       "SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]",
                                       "FROM [L0].[" + self.get_table_name(table_name) + "] [sn]",
                                       "CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]", "where [Exec_Id] = @Exec_Id"])
        sql_query += self.ut.concat_strs(1, 2, ["AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore",
                                        "AND [scarti].[ID_Flusso] = @ID_Flusso", "AND [scarti].[Flag_Enabled] = 1",
                                        "AND ("])

        dates = ["--OR ([sn].[" + str(name_col) + "] is null OR [sn].[" + str(name_col) + "] = '')" for name_col in
                 info.loc[info["Tipo"] == "date"].NomeColonna]
        dates[0] = "--" + dates[0][5:]
        sql_query += self.ut.concat_strs(1, 3, [
            "--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione"] + dates)

        sql_query += self.ut.concat_strs(1, 2, [")"])
        sql_query += self.ut.concat_strs(1, 1, [";"])

        return sql_query + " \n" * 2

    def INVALID_DATE_case(self, info, table_name):
        sql_query = self.ut.concat_strs(1, 1, ["--Esclusione date non Valide",
                                       "SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'", ";",
                                       "UPDATE [sn]",
                                       "SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]",
                                       "FROM [L0].[" + self.get_table_name(table_name) + "] [sn]",
                                       "CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]", "where [Exec_Id] = @Exec_Id"])
        sql_query += self.ut.concat_strs(1, 2, ["AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore",
                                        "AND [scarti].[ID_Flusso] = @ID_Flusso", "AND [scarti].[Flag_Enabled] = 1",
                                        "AND ("])

        dates = [
            "OR ([sn].[" + str(name_col) + "] is not null AND [sn].[" + str(name_col) + "]  <> '' AND " + self.conv_date(
                "[sn].[" + str(name_col) + "]") + " IS NULL)" for name_col in
            info.loc[info["Tipo"] == "date"].NomeColonna]
        dates[0] = dates[0][3:]
        sql_query += self.ut.concat_strs(1, 3, [
            "--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione"] + dates)

        sql_query += self.ut.concat_strs(1, 2, [")"])
        sql_query += self.ut.concat_strs(1, 1, [";"])

        return sql_query + " \n" * 2

    def EMPTY_NUMERIC_case(self, info, table_name):
        sql_query = self.ut.concat_strs(1, 1, ["SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'", ";",
                                       "UPDATE [sn]",
                                       "SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]",
                                       "FROM [L0].[" + self.get_table_name(table_name) + "] [sn]",
                                       "CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]", "where [Exec_Id] = @Exec_Id"])
        sql_query += self.ut.concat_strs(1, 2, ["AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore",
                                        "AND [scarti].[ID_Flusso] = @ID_Flusso", "AND [scarti].[Flag_Enabled] = 1",
                                        "AND ("])

        numerics = ["--OR ([sn].[" + str(name_col) + "] is null )" for name_col in
                    info.loc[info["Tipo"].isin(["numeric", "bit", "int"])].NomeColonna]
        numerics[0] = "--" + numerics[0][5:]
        sql_query += self.ut.concat_strs(1, 3, [
            "--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione"] + numerics)

        sql_query += self.ut.concat_strs(1, 2, [")"])
        sql_query += self.ut.concat_strs(1, 1, [";"])

        return sql_query + " \n" * 2

    def INVALID_NUMERIC_case(self, info, table_name):
        sql_query = self.ut.concat_strs(1, 1, ["--Esclusione numeric non Validi",
                                       "SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'", ";",
                                       "UPDATE [sn]",
                                       "SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]",
                                       "FROM [L0].[" + self.get_table_name(table_name) + "] [sn]",
                                       "CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]", "where [Exec_Id] = @Exec_Id"])
        sql_query += self.ut.concat_strs(1, 2, ["AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore",
                                        "AND [scarti].[ID_Flusso] = @ID_Flusso", "AND [scarti].[Flag_Enabled] = 1",
                                        "AND ("])

        numerics = ["OR ([sn].[" + str(row.NomeColonna) + "] is NOT null AND " + self.conv_numeric(row) for _, row in
                    info.loc[info["Tipo"].isin(["numeric", "bit", "int"])].iterrows()]
        numerics[0] = numerics[0][3:]
        sql_query += self.ut.concat_strs(1, 3, [
            "--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione"] + numerics)

        sql_query += self.ut.concat_strs(1, 2, [")"])
        sql_query += self.ut.concat_strs(1, 1, [";"])

        return sql_query + " \n" * 2

    def insert_into_SCARTI(self, info, table_name):
        sql_query = self.ut.concat_strs(1, 1, ["BEGIN TRANSACTION",
                                       "SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'",
                                       ";", "INSERT INTO [L0_SCARTI].[" + self.get_table_name(table_name) + "]("])

        columns = [",[" + str(NomeColonna) + "]" for NomeColonna in info.NomeColonna]
        columns[0] = columns[0][1:]
        sql_query += self.ut.concat_strs(1, 2, columns + [",[Row_Id]", ",[Exec_Id]", ",[Bitmask_Scarti])"])

        sql_query += self.ut.concat_strs(1, 1, ["select"])
        sql_query += self.ut.concat_strs(1, 2, columns + [",[Row_Id]", ",[Exec_Id]", ",[Bitmask_Scarti]"])

        sql_query += self.ut.concat_strs(1, 1, ["FROM [L0].[" + self.get_table_name(table_name) + "]", "WHERE [Exec_Id] = @Exec_Id",
                                        "AND  [BitMask_Scarti] <> 0", ";"])

        return sql_query + " \n" * 2

    def merge(self, info, table_name):
        sql_query = self.ut.concat_strs(1, 1, ["SET @Step = '4. Esecuzione Merge'", ";",
                                       "MERGE [L1].[" + self.get_table_name(table_name) + "] AS dst", "USING "])
        sql_query += self.ut.concat_strs(1, 2, ["( SELECT"])

        aux_str = [str(self.merge_row(row)) for _, row in info.iterrows()]
        aux_str[0] = aux_str[0][1:]
        sql_query += self.ut.concat_strs(1, 3, aux_str + [",[Row_Id]"])

        sql_query += self.ut.concat_strs(1, 2, ["FROM [L0].[" + self.get_table_name(table_name) + "]", "WHERE ["
                                                                                                       "Exec_Id] = "
                                                                                                       "@Exec_Id",
                                        "AND [BitMask_Scarti] = 0", ") AS src "])

        if len(info.loc[info["Key"]]) != 0:
            aux_str = ["AND [src].[" + nome + "] = [dst].[" + nome + "]" for nome in info.loc[info.Key].NomeColonna]
            aux_str[0] = "on" + aux_str[0][3:]
            sql_query += self.ut.concat_strs(1, 3, aux_str)
        else:
            sql_query += self.ut.concat_strs(1, 3, ["-- Inserire qui le chiavi nella forma: on [src].[key] = [dst].[key]  \n\t\t\t -- AND [src].[key] = [dst].[key]"])

        sql_query += self.ut.concat_strs(1, 1, ["WHEN not matched THEN INSERT ("])

        aux_str = [",[" + nome + "]" for nome in info.NomeColonna]
        aux_str[0] = aux_str[0][1:]
        sql_query += self.ut.concat_strs(1, 3,
                                 aux_str + [",[Exec_Id_InsertedOn]", ",[DateTime_InsertedOn]", ",[Row_Id_InsertedOn] "])

        sql_query += self.ut.concat_strs(1, 2, [") VALUES ("])

        aux_str = [",[src].[" + nome + "]" for nome in info.NomeColonna]
        aux_str[0] = aux_str[0][1:]
        sql_query += self.ut.concat_strs(1, 3, aux_str + [",@Exec_Id", ",@Now", ",[src].[Row_Id])"])

        sql_query += self.ut.concat_strs(1, 1, ["WHEN matched THEN UPDATE SET"])
        aux_str = [",[" + nome + "] = [src].[" + nome + "]" for nome in info.loc[~ info.Key].NomeColonna]
        aux_str[0] = aux_str[0][1:]
        sql_query += self.ut.concat_strs(1, 3, aux_str + [",[Exec_Id_UpdatedOn] = @Exec_Id", ",[DateTime_UpdatedOn] = @Now",
                                                  ",[Row_Id_UpdatedOn]  = [src].[Row_Id]"])
        sql_query += self.ut.concat_strs(1, 1, [";", "COMMIT TRANSACTION"])

        return sql_query + "\n" * 2

    def catch_ex(self):
        sql_query = self.ut.concat_strs(1, 1, ["END TRY", "BEGIN CATCH"])
        sql_query += self.ut.concat_strs(1, 2, ["IF @@TRANCOUNT > 0", "BEGIN", "\tROLLBACK TRANSACTION", "END", ";",
                                        "DECLARE @Message VARCHAR(MAX) = 'STEP ' + @step + ' ____'+ ERROR_MESSAGE() + '____ '",
                                        ";", "RAISERROR (@Message, 16,1)", ";", ""])
        sql_query += self.ut.concat_strs(1, 1, ["END CATCH"])
        return sql_query

    def create_usp(self, table_info, table_name):
        sql_query = self.prepare_comment(table_name)
        sql_query += self.prepare_signature(table_name)
        sql_query += self.prepare_declare()
        sql_query += self.get_info_from_table(table_name)

        # Controlli sulle chiavi, se non sono presenti le chiavi il codice relativo viene commentato

        # Controllo momentaneamente self.ut.commentato
        sql_query_key = self.ut.commenta(self.DUPLICATE_KEY_case(table_info, table_name))
        sql_query_key += self.GET_EMPTY_KEY_case(table_info, table_name)
        sql_query_key += self.GET_ONE_DUPLICATE_KEY_case(table_info, table_name)

        if len(table_info.loc[table_info["Key"]]) == 0:
            sql_query_key = self.ut.commenta(sql_query_key)

        sql_query += sql_query_key

        # Controlli sulle date.
        # I controlli che i campi date non sia nulli sono self.ut.commentati
        # Tutte le date non nulle vengono controlalte
        if len(table_info.loc[table_info["Tipo"] == "date"]) > 0:
            sql_query_date = self.EMPTY_DATE_case(table_info, table_name)
            sql_query_date += self.INVALID_DATE_case(table_info, table_name)
            sql_query += sql_query_date

        # Controlli sui numeric/int/bit.
        # I controlli che i campi numeric/int/bit non sia nulli sono self.ut.commentati
        # Tutte i numeric/int/bit non nulli vengono controlalte
        if len(table_info.loc[table_info["Tipo"].isin(["numeric", "bit", "int"])]) > 0:
            sql_query_numeric = self.EMPTY_NUMERIC_case(table_info, table_name)
            sql_query_numeric += self.INVALID_NUMERIC_case(table_info, table_name)
            sql_query += sql_query_numeric

        # Inserimento dati nella tabelle SCARTI
        sql_query += self.insert_into_SCARTI(table_info, table_name)

        # fase di Merge
        sql_query += self.merge(table_info, table_name)

        # gestione eccezioni
        sql_query += self.catch_ex()

        self.ut.write_sql_query(sql_query, table_name, "usp_Load_" + self.get_table_name(table_name) + ".sql")
        self.sqlQuery += "\n\n\n --Procedura per creare la Load usp \n" + sql_query

