CREATE TABLE [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF]( 
		[COD_ABI] [varchar] (5) NULL,
		[COD_NDG_CONTRATTUALE] [varchar] (13) NULL,
		[COD_MANDATO] [varchar] (5) NULL,
		[COD_FILIALE_NDG_CONTRATTO] [varchar] (5) NULL,
		[COD_FILIALE_PORTAFOGLIO] [varchar] (5) NULL,
		[COD_PORTAFOGLIO] [varchar] (2) NULL,
		[COD_TIPO_PORTAFOGLIO] [varchar] (1) NULL,
		[DAT_INIZIO_ASSOCIAZIONE_PTF] [varchar] (10) NULL,
		[DAT_FINE_ASSOCIAZIONE_PTF] [varchar] (10) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 