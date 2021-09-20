CREATE TABLE [L0].[T_Vita_QUOTE_FONDI]( 
		[COD_ABI] [varchar] (5) NULL,
		[COD_CONTRATTO] [varchar] (34) NULL,
		[COD_COMPAGNIA] [varchar] (5) NULL,
		[COD_POLIZZA] [varchar] (11) NULL,
		[COD_PRODOTTO] [varchar] (5) NULL,
		[COD_TARIFFA_ENTE] [varchar] (5) NULL,
		[COD_PRODOTTO_INTERNO] [varchar] (20) NULL,
		[COD_GARANZIA] [varchar] (5) NULL,
		[COD_FONDO] [varchar] (5) NULL,
		[DAT_AVVALORAMENTO] [varchar] (20) NULL,
		[NUM_QUOTE_OPERAZIONE] [varchar] (8) NULL,
		[IMP_CONTROVALORE_QUOTE] [varchar] (26) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 