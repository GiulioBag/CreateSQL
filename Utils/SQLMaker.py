from .UspMaker import UspMaker
from .LoaderMaker import LoaderMaker


class SQLMaker:

    def __init__(self, ut, table_info, table_name, ambito):

        self.ut = ut
        self.table_info = table_info
        self.table_name = table_name
        self.uspMaker = UspMaker(ut)
        self.sqlQuery = ""
        self.ambito = ambito
        #self.loader = LoaderMaker()


    def create_table(self, schema):

        aux_name = "_".join(self.table_name.split(" "))

        return "CREATE TABLE [" + schema + "].[T_" + self.ut.ambito + "_" + aux_name + "]( \n"

    def create_L0(self):
        sql_query = self.create_table("L0")

        for _, row in self.table_info.iterrows():
            if isinstance(row.Lunghezza, int):
                lunghezza = row.Lunghezza

            elif isinstance(row.Lunghezza, list):
                lunghezza = int(row.Lunghezza[0]) + int(row.Lunghezza[1])
            else:
                lunghezza = None
            sql_query += "\t\t[" + row.NomeColonna + "] [varchar] (" + str(lunghezza) + ") NULL,\n"

        sql_query += "\t\t[Row_Id] [int] IDENTITY(1,1) NOT NULL, \n\t\t[Exec_Id] [int] NULL,\n\t\t[Bitmask_Scarti] [" \
                     "bigint] NULL\n) ON [PRIMARY] "

        flusso = "_".join(self.table_name.split(" "))
        self.ut.write_sql_query(sql_query, flusso, "create_L0_" + flusso + ".sql")

        self.sqlQuery += " -- Procedura per creare L0\n" +  sql_query + "\nGO"

    def create_L0_SCARTI(self):
        sql_query = self.create_table("L0_SCARTI")

        for _, row in self.table_info.iterrows():
            if isinstance(row.Lunghezza, int):
                lunghezza = row.Lunghezza
            elif isinstance(row.Lunghezza, list):
                lunghezza = int(row.Lunghezza[0]) + int(row.Lunghezza[1])
            else:
                lunghezza = None
            sql_query += "\t\t[" + row.NomeColonna + "] [varchar] (" + str(lunghezza) + ") NULL,\n"

        sql_query += "\t\t[Row_Id] [int] NULL, \n\t\t[Exec_Id] [int] NULL,\n\t\t[Bitmask_Scarti] [bigint] NULL\n) ON " \
                     "[PRIMARY] "

        flusso = "_".join(self.table_name.split(" "))
        self.ut.write_sql_query(sql_query, flusso, "create_L0_SCARTI_" + flusso + ".sql")

        self.sqlQuery += "\n\n\n --Procedura per creare L0 SCARTI \n" + sql_query + "\nGO"

    def create_L1(self):
        sql_query = self.create_table("L1") + "\t\t"
        costraints = ""
        for _, row in self.table_info.iterrows():

            if row.Tipo == "varchar":
                lunghezza = "(" + str(row.Lunghezza) + ") "
            elif row.Tipo == "numeric":
                if isinstance(row.Lunghezza, int):
                    row.Lunghezza = [row.Lunghezza, 0]
                lunghezza = "(" + str(row.Lunghezza[0]) + ", " + str(row.Lunghezza[1]) + ") "
            else:
                lunghezza = ""

            if row.Key:
                end_str = "NOT NULL, \n\t\t"
                costraints += "\t\t[" + row.NomeColonna + "] ASC,\n"
            else:
                end_str = "NULL, \n\t\t"

            sql_query += "[" + row.NomeColonna + "] [" + str(row.Tipo) + "] " + lunghezza + end_str

        sql_query += "[Exec_Id_InsertedOn] [int] NULL,\n\t\t[DateTime_InsertedOn] [datetime] NULL," \
                     "\n\t\t[Row_Id_InsertedOn] [int] NULL,\n\t\t[Exec_Id_UpdatedOn] [int] NULL," \
                     "\n\t\t[DateTime_UpdatedOn] [datetime] NULL,\n\t\t[Row_Id_UpdatedOn] [int] NULL\n "
        if len(self.table_info.loc[self.table_info.Key]) != 0:
            sql_query += "CONSTRAINT [PK_L1_T_" + "_".join([self.ambito] +
                self.table_name.split(" ")) + "] PRIMARY KEY CLUSTERED \n(" + costraints[:-2] + "\n"
            sql_query += ")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = " \
                         "ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\n) ON [PRIMARY] "
        else:
            sql_query += ") ON[PRIMARY]"

        flusso = "_".join(self.table_name.split(" "))
        self.ut.write_sql_query(sql_query, flusso, "create_L1_" + flusso + ".sql")

        self.sqlQuery += "\n\n\n --Procedura per creare L1 \n" + sql_query + "\nGO"

    def create_view_from(self, schema):
        aux_name = "_".join(self.table_name.split(" "))
        return "CREATE VIEW [" + schema + "].[V_" + self.ut.ambito + "_" + aux_name + "]\t AS", \
               "FROM [" + schema + "].[T_" + self.ut.ambito + "_" + aux_name + "] as [r] "

    def view_builder(self):
        sql_query = self.create_view_from("L1")[0]
        sql_query += self.ut.concat_strs(1, 1, ["SELECT "])
        columns = [",[r].[" + str(NomeColonna) + "]" for NomeColonna in self.table_info.NomeColonna]
        columns[0] = columns[0][1:]
        sql_query += self.ut.concat_strs(1, 2, columns)
        sql_query += self.ut.concat_strs(2, 1, [self.create_view_from("L1")[1]])

        flusso = "_".join(self.table_name.split(" "))
        self.ut.write_sql_query(sql_query, flusso, "create_VIEW_" + flusso + ".sql")
        self.sqlQuery += "\n\n\n --Procedura per creare la vista \n" + sql_query + "\nGO"
