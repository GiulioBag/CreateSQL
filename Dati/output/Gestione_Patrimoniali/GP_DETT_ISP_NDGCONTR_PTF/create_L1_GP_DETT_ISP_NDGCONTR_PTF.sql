CREATE TABLE [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF]( 
		[COD_ABI] [varchar] (5) NULL, 
		[COD_NDG_CONTRATTUALE] [varchar] (13) NULL, 
		[COD_MANDATO] [varchar] (5) NULL, 
		[COD_FILIALE_NDG_CONTRATTO] [varchar] (5) NULL, 
		[COD_FILIALE_PORTAFOGLIO] [varchar] (5) NULL, 
		[COD_PORTAFOGLIO] [varchar] (2) NULL, 
		[COD_TIPO_PORTAFOGLIO] [varchar] (1) NULL, 
		[DAT_INIZIO_ASSOCIAZIONE_PTF] [date] NULL, 
		[DAT_FINE_ASSOCIAZIONE_PTF] [date] NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 ) ON[PRIMARY]