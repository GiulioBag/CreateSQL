CREATE TABLE [L0].[T_Vita_Fondi_Polizze]( 
		[COD_ABI] [varchar] (5) NULL,
		[COD_CONTRATTO] [varchar] (34) NULL,
		[COD_FONDO] [varchar] (5) NULL,
		[COD_COMPAGNIA] [varchar] (5) NULL,
		[DAT_EMISSIONE] [varchar] (10) NULL,
		[DATA_QUOTAZIONE] [varchar] (10) NULL,
		[IMP_QUOTA] [varchar] (22) NULL,
		[NUM_QUOTE_OPERAZIONE] [varchar] (8) NULL,
		[IMP_CONTROVALORE] [varchar] (22) NULL,
		[PERC_CONTROVALORE] [varchar] (22) NULL,
		[DAT_AVVALORAMENTO] [varchar] (10) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 