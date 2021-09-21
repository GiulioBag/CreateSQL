import os
import ast
import re

class Utils:

    def __init__(self, path="Dati//Const"):
        var_dict = self.read_file(path)
        
        self.output_path = var_dict["OUTPUT_PATH"]
        self.input_path = var_dict["INPUT_PATH"]

        self.input_name = ""
        self.sheet_name = ""

        #self.ambito = var_dict["AMBITO"]
        self.ambito = ""

        self.codifica_data = var_dict["CODIFICA_DATA"]

        #self.sheet_to_do = var_dict["sheet_to_do"]

        self.data_ExcelCols = var_dict["data_ExcelCols"]

        self.tipiSql_nomiCol = var_dict["tipiSql_nomiCol"]

        self.predict_values = var_dict["predict_values"]

        self.tipiSql_nomiTipi= var_dict["tipiSql_nomiTipi"]

        self.tipiSql_defaultLen = var_dict["tipiSql_defaultLen"]

        self.NomiCella_ConNome_Tabella = var_dict["NomiCella_ConNome_Tabella"]

    def read_file(self, path):
        """
        Metodo per recuperare i valori delle costanti
        :param path: Path dove trovare il file confing.txt
        :return: Un dizionario contenente (costante - valore)
        """
        f = open(self.join_paths([path, "config.txt"]), "r")
        if f.mode == 'r':
            var_dict = f.read()
            var_dict = ast.literal_eval(var_dict)
            return var_dict
        else:
            print("File config.txt reading error.")

    def join_paths(self, names):
        path = ""
        for name in names:
            path = os.path.join(path, name)
        return path
    def write_sql_query(self, str_query, flusso, name_file):
        if not os.path.isdir(self.join_paths([self.output_path, self.ambito])):
            os.mkdir(self.join_paths([self.output_path, self.ambito]))

        if not os.path.isdir(self.join_paths([self.output_path, self.ambito, flusso])):
            os.mkdir(self.join_paths([self.output_path, self.ambito, flusso]))

        f = open(self.join_paths([self.output_path, self.ambito, flusso, name_file]), "w")
        f.write(str_query)
        f.close()
    def concat_strs(self, num_n, num_t, strs):
        aux = ""
        for s in strs:
            aux += "\n" * num_n + "\t" * num_t + s
        return aux

    def commenta(self, txt):
        return txt.replace("\n", "\n--")

    def normalize_str(self, str):
        return str.lower().replace(" ", "").replace("_", "").replace("-", "")

    def get_table_name(self, df, sheet_name):
        """
        Determina il nome della tabella con la seguente logica:
            - Se il nome del foglio non rispetta la rege "Sheet\{d}+" usiamo quello
            - Se la rispetta cerchiamo all'interno del foglio informazioni sul nome della tabella
        :param df: foglio excel
        :param sheet_name:
        :return: Nome della tabella associata al foglio
        """
        regex_en = re.compile(r"Sheet[0-9]+")
        regex_it = re.compile(r"Foglio[0-9]+")
        if not re.fullmatch(regex_en, sheet_name) and not re.fullmatch(regex_it, sheet_name):
            return sheet_name.replace(" ", "_")
        else:
           # Check nel nome delle colonne
            columns = list(df.columns)
            for i in range(len(columns) - 1):
                if self.normalize_str(columns[i]) in self.NomiCella_ConNome_Tabella:
                    name = columns[i + 1]
                    return name.split(".")[0].replace(" ", "_")
            # TODO Se il nome della tabella non è nella prima riga



