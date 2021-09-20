import pandas as pd
from Utils import Utils
import re


class DfMaker:

    def __init__(self, ut: Utils, excel_sheet: pd.DataFrame):
        self.ut = ut
        self.excel_sheet = excel_sheet

    def get_info(self):
        """
        Elimina le colonne e le righe dal foglio excel non utili
        :return: un df contenente solo le info utili
        """

        # Seleziono solo le colonne corrette
        df_to_return = self.get_columns()

        # Seleziono solo le righe corrette e "casto" ai tipi di interesse
        df_to_return = self.get_rows(df_to_return)

        # Eseguo la merge
        df_to_return = self.merge_columns(df_to_return)

        # Se le opzioni avanzate sono abilitate cerco di predirre i dati mancanti o errati
        # basandomi sul nome della colonna, sulla sua dimensione e sul tipo
        if self.ut.predict_values:
            df_to_return = self.predict_values(df_to_return)

        df_to_return.to_csv(self.ut.output_path + "\df.csv", index=False)

        new_names = list(df_to_return.NomeColonna)
        df_to_return.NomeColonna = [nome.replace(" ", "_") for nome in new_names]

        return df_to_return

    def get_columns(self):
        # Controllo che nella prima dello sheet ci siano le colonne volute

        code, df = self.check_columns()
        if code == 0:
            return df

        # Nella prima riga dello sheet non sono state trovate le colonne di interesse, scorriamo le righe dello sheet
        # alla ricerca di una riga che contenga tutte le colonne volute

        len_excel = len(self.excel_sheet)
        for _ in range(len_excel - 1):
            new_cols = [str(v) for v in self.excel_sheet.iloc[[0]].values[0]]

            self.excel_sheet = self.excel_sheet.iloc[1:, :]
            self.excel_sheet.columns = new_cols

            code, df = self.check_columns()
            if code == 0:
                return df

        raise Exception("Non è stato possibile trovare le colonne corrette nel file " + self.ut.input_name + " nel foglio: " + self.ut.sheet_name)

    def check_columns(self):
        """
            Controllo che le colonne siano quelle volute.
            Per farlo scorro ut.data_ExcelCols e segno tutte le volte che ho trovato una nuova colonna.
            Se una colonna voluta appare più di una volta prendo tutte le colonne doppioni.
            :param df: df in oggetto
            :return:
                    Caso Positivo: 0, un df con le colonne corrette
                    Caso Negativo: -1, None
        """

        df_to_return = pd.DataFrame()
        # Conterrà come chiave il nome vero trovato e come valore la lista dei possibili nomi del foglio che matchano
        count_dict = dict()

        for col_name in self.excel_sheet.columns:
            for k, v in self.ut.data_ExcelCols.items():
                if self.ut.normalize_str(col_name) in v:

                    if k not in count_dict.keys():
                        count_dict[k] = []
                    # Mi segno il nome della colonna, se ne ho più di una potrei dover fare il merge
                    count_dict[k].append(col_name)
                    break

        # Controllo che il numero delle chaivi di count_dict sia uguale a quello di self.ut.data_ExcelCols,
        # in questo caso abbiamo trovato tutte le colonne

        if len(self.ut.data_ExcelCols.keys()) == len(count_dict.keys()):
            # Devo controllare se fare la merge o meno
            for k, v in count_dict.items():
                if len(v) != 1:
                    for it, excel_column_name in enumerate(v):
                        # Mi salvo tutte le colonne doppioni cambiandone il nome
                        df_to_return[k + "_" + str(it)] = list(self.excel_sheet[excel_column_name])
                else:
                    df_to_return[k] = list(self.excel_sheet[v[0]])
            return 0, df_to_return
        return -1, None

    def get_rows(self, df: pd.DataFrame):
        """
        Esclude le righe in cui manca il campo "Nome Colonna" (controlla eventualemnte che vi siano pià campi con
        questo nome).
        Mappa i valori nei codici da usare nel SQL
        :param df:
        :return:
        """

        index_to_drop = []
        col_names = df.columns
        for index, row in df.iterrows():

            nome_colonna_count = 0

            for col in col_names:
                real_col_name = col.split("_")[0]

                # Se manca il nome dela colonna escludiamo la riga
                if real_col_name == "NomeColonna" and row[col] is None:
                    nome_colonna_count += 1
                    if nome_colonna_count == sum([1 if ("NomeColonna" in name_col) else 0 for name_col in col_names]):
                        index_to_drop.append(index)
                    break

                # Se manca un valore la colonna non è chiave
                if real_col_name == "Key":
                    if row[col] is None:
                        df.at[index, col] = False
                    else:
                        df.at[index, col] = True

                if real_col_name == "Lunghezza":
                    # Controllo quanti valori numerici sono contenuti all'interno
                    values = re.findall(r'\d+', str(row[col]))

                    # Codifica errata
                    if len(values) == 0 or len(values) > 2:
                        df.at[index, col] = None
                    # E' un int/bit/varchar
                    elif len(values) == 1:
                        df.at[index, col] = int(values[0])
                    # E' un numeric
                    else:
                        df.at[index, col] = [int(values[0]), int(values[1])]

                if real_col_name == "Tipo":
                    new_tipo = None
                    for k, v in self.ut.tipiSql_nomiTipi.items():
                        if self.ut.normalize_str(str(row[col])) in v:
                            new_tipo = k
                            break

                    df.at[index, col] = new_tipo

        if len(index_to_drop) != 0:
            df = df.drop(index_to_drop)
        return df

    def merge_columns(self, df: pd.DataFrame):
        """
        Partendo dai nomi delle colonne del df determina il volaro di una nuova colonna, a seconda della colonna di
        destinazione sono usate logiche diverse:

        real_name = Lunghezza:
            - Se è presente un solo valore numerico prendo quello.
            - Se sono presenti più valori numerici prendo quello più grande, se uno di essi è una lista prendo quella

        real_name = altro:
            - Se è presente un solo valore prendo quello.
            - Lancio un'eccezione

        :param df:
        :return: Una lista contenente i valori veri
        """
        df_to_return = pd.DataFrame()


        for real_col_name in self.ut.data_ExcelCols.keys():

            vals_to_col = []
            #Numero delle volte in cui compare un'istanza della colonna nel df
            num_col = sum([1 if real_col_name == col_name.split("_")[0] else 0 for col_name in df.columns])

            if num_col == 1:
                vals_to_col = list(df[real_col_name])

           # Gestione della real_col Lunghezza
            elif real_col_name == "Lunghezza":
                for _, row in df.iterrows():
                    values = [row["Lunghezza_" + str(i)] for i in range(num_col)]

                    # Controllo che via sia un solo valore
                    good_vals = [v for v in values if v is not None]
                    if len(good_vals) == 1:
                        vals_to_col.append(good_vals[0])

                    elif len(good_vals) == 0:
                        vals_to_col.append(None)
                    else:
                    # Prendo eventuali liste
                        lists = [v for v in good_vals if isinstance(v, list)]
                        if len(lists) == 0:
                            vals_to_col.append(max(good_vals))
                        elif len(lists) == 1:
                            vals_to_col.append(lists[0])
                        else:
                            # Caso in cui ho più di una lista, prendo la lista che alloca più spazio
                            spaces = [l[0] for l in lists]
                            vals_to_col.append(lists[spaces.index(max(spaces))])
            #Gestione altre real_col
            else:
                for index, row in df.iterrows():
                    values = [row[real_col_name + "_" + str(i)] for i in range(num_col)]
                    good_vals = [v for v in values if v is not None]

                    if len(good_vals) == 1:
                        vals_to_col.append(good_vals[0])
                    else:
                        raise Exception("Per la colonna: " + real_col_name + " alla riga: " + str(index) + " sono presenti più valori validi")

            df_to_return[real_col_name] = vals_to_col

        return  df_to_return

    def predict_values(self, df):

    # Divido il nome della colonna, se questo contiene parole che possono farci pensare che appartiene ad un altro tipo
    # setto quest'ultimo modificando anche la sua lunghezza

        new_Lunghezza = []
        for ele in list(df.Lunghezza):
            if not isinstance(ele, list) and pd.isna(ele):
                new_Lunghezza.append(ele)
            elif isinstance(ele, list):
                new_Lunghezza.append([int(ele[0]), int(ele[1])])
            else:
                new_Lunghezza.append(int(ele))

        for index, row in df.iterrows():
            name = row.NomeColonna
            for par in "_".join(name.split(" ")).split("_"):

                for k, v in self.ut.tipiSql_nomiCol.items():
                    if self.ut.normalize_str(par) in v and df.at[index, "Tipo"] != k:

                        #Se ho un numeric con decimali e il nome ci indica che dobbiamo cambiare il tipo NON lo cambiamo
                        if not (row.Tipo == "numeric" and isinstance(row.Lunghezza, list)):
                            df.at[index, "Tipo"] = k
                            new_Lunghezza[index] = self.ut.tipiSql_defaultLen[k]
                            #df.at[index, "Lunghezza"] = self.ut.tipiSql_defaultLen[k]

        df["Lunghezza"] = new_Lunghezza
    # Se abbiamo un valore numerci la cui lughezza è indicaata da un solo valore si effettua il cast ad intero
        for index, row in df.iterrows():
            if row.Tipo == "numeric" and not isinstance(row.Lunghezza, list):
                df.at[index, "Tipo"] = "int"


        return df

