CREATE TABLE [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_SOA_FILRAPP]( 
		[COD_ABI] [varchar] (5) NULL, 
		[COD_FILIALE_RAPPORTO] [varchar] (5) NULL, 
		[DES_FILIALE_RAPPORTO] [varchar] (100) NULL, 
		[DIREZIONE_REGIONALE] [varchar] (100) NULL, 
		[COD_AREA] [varchar] (5) NULL, 
		[DES_AREA] [varchar] (100) NULL, 
		[NOM_INDIRIZZO_FILIALE_UFFICIO] [varchar] (100) NULL, 
		[NOM_COMUNE_FILIALE_UFFICIO] [varchar] (100) NULL, 
		[GEO_COORDINATA_X] [numeric] (11, 6) NULL, 
		[GEO_COORDINATA_Y] [numeric] (11, 6) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 ) ON[PRIMARY]