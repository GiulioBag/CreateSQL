CREATE TABLE [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_SALDI_MENSILI]( 
		[COD_ABI] [varchar] (5) NULL, 
		[COD_CONTRATTO] [varchar] (34) NULL, 
		[COD_MANDATO] [varchar] (5) NULL, 
		[ANNO_MESE_SALDO] [int] NULL, 
		[IMP_SALDO_GP] [numeric] (20, 3) NULL, 
		[PRC_RENDIMENTO_ANNUALIZZATO] [numeric] (17, 9) NULL, 
		[PRC_RENDIMENTO_ANNUO] [numeric] (17, 9) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 ) ON[PRIMARY]