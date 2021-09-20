CREATE TABLE [L1].[T_Vita_Struttura_Organizzativa]( 
		[ANNOMESE] [int] NULL, 
		[COD_DIREZIONE_REGIONALE] [varchar] (5) NULL, 
		[DES_DIREZIONE_REGIONALE] [varchar] (100) NULL, 
		[NOM_INDIRIZZO_DIR_REGIONALE] [varchar] (100) NULL, 
		[NOM_COMUNE_DIR_REGIONALE] [varchar] (100) NULL, 
		[COD_CAP_DIR_REGIONALE] [varchar] (5) NULL, 
		[COD_PROVINCIA_DIR_REGIONALE] [varchar] (2) NULL, 
		[COD_DIREZIONE_COMMERCIALE] [varchar] (5) NULL, 
		[DES_DIREZIONE_COMMERCIALE] [varchar] (100) NULL, 
		[NOM_INDIRIZZO_DIR_COMMERCIALE] [varchar] (100) NULL, 
		[NOM_COMUNE_DIR_COMMERCIALE] [varchar] (100) NULL, 
		[COD_CAP_DIR_COMMERCIALE] [varchar] (5) NULL, 
		[COD_PROVINCIA_DIR_COMMERCIALE] [varchar] (2) NULL, 
		[COD_AREA] [varchar] (5) NULL, 
		[DES_AREA] [varchar] (100) NULL, 
		[NOM_INDIRIZZO_AREA] [varchar] (100) NULL, 
		[NOM_COMUNE_AREA] [varchar] (100) NULL, 
		[COD_CAP_AREA] [varchar] (5) NULL, 
		[COD_PROVINCIA_AREA] [varchar] (2) NULL, 
		[COD_ABI] [varchar] (5) NULL, 
		[COD_FILIALE_UFFICIO] [varchar] (5) NULL, 
		[DES_FILIALE_UFFICIO] [varchar] (100) NULL, 
		[COD_DW_FILIALE] [int] NULL, 
		[NOM_INDIRIZZO_FILIALE_UFFICIO] [varchar] (100) NULL, 
		[NOM_LOCALITA_FILIALE_UFFICIO] [varchar] (100) NULL, 
		[NOM_COMUNE_FILIALE_UFFICIO] [varchar] (100) NULL, 
		[COD_CAP_FILIALE_UFFICIO] [varchar] (5) NULL, 
		[NOM_PROVINCIA_FILIALE_UFFICIO] [varchar] (50) NULL, 
		[GEO_COORDINATA_X] [numeric] (11, 6) NULL, 
		[GEO_COORDINATA_Y] [numeric] (11, 6) NULL, 
		[COD_CLASSE_FILIALE_UFFICIO] [varchar] (2) NULL, 
		[DES_CLASSE_FILIALE_UFFICIO] [varchar] (100) NULL, 
		[TERRITORIO] [varchar] (13) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 ) ON[PRIMARY]