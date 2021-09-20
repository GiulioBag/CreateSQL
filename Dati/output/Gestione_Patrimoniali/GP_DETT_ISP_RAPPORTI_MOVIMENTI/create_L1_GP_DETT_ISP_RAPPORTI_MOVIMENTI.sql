CREATE TABLE [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI]( 
		[COD_ABI] [varchar] (5) NULL, 
		[COD_CONTRATTO] [varchar] (34) NULL, 
		[COD_MANDATO] [varchar] (5) NULL, 
		[COD_FILIALE_RAPPORTO] [varchar] (5) NULL, 
		[COD_NDG_CONTRATTUALE] [varchar] (13) NULL, 
		[DAT_APERTURA_CONTRATTO] [date] NULL, 
		[DAT_CHIUSURA_CONTRATTO] [date] NULL, 
		[COD_STATO_CONTRATTO_GP] [varchar] (1) NULL, 
		[DES_STATO_CONTRATTO_GP] [varchar] (100) NULL, 
		[COD_SGR] [varchar] (5) NULL, 
		[DES_SOCIETA_GESTIONE_RISPARMIO] [varchar] (100) NULL, 
		[IMP_INIZIALE_INVESTITO] [numeric] (20, 3) NULL, 
		[IMP_SOGLIA_INIZIO_CONFERIMENTO] [numeric] (20, 3) NULL, 
		[IMP_SOGLIA_INIZIALE_SWITCH] [numeric] (20, 3) NULL, 
		[IMP_SOGLIA_VARZN_CONFRMNT] [numeric] (26, 3) NULL, 
		[DAT_PRIMO_CONFERIMENTO_PRD] [date] NULL, 
		[DAT_ULTIMO_CONFERIMENTO_PRD] [date] NULL, 
		[COD_LINEA_PRODOTTO] [varchar] (2) NULL, 
		[DES_LINEA_PRODOTTO] [varchar] (100) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 ) ON[PRIMARY]