CREATE TABLE [L0_SCARTI].[T_Gestione_Patrimoniali_GP_DETT_ISP_SALDI_MENSILI]( 
		[COD_ABI] [varchar] (5) NULL,
		[COD_CONTRATTO] [varchar] (34) NULL,
		[COD_MANDATO] [varchar] (5) NULL,
		[ANNO_MESE_SALDO] [varchar] (8) NULL,
		[IMP_SALDO_GP] [varchar] (23) NULL,
		[PRC_RENDIMENTO_ANNUALIZZATO] [varchar] (26) NULL,
		[PRC_RENDIMENTO_ANNUO] [varchar] (26) NULL,
		[Row_Id] [int] NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 