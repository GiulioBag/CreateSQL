CREATE TABLE [L0].[T_Vita_Polizze_Prodotto]( 
		[COD_ABI] [varchar] (5) NULL,
		[COD_COMPAGNIA] [varchar] (5) NULL,
		[COD_NATURA] [varchar] (3) NULL,
		[COD_RAMO_ASSICURATIVO] [varchar] (5) NULL,
		[COD_TARIFFA_ENTE] [varchar] (5) NULL,
		[COD_PRODOTTO_INTERNO] [varchar] (5) NULL,
		[COD_PRODOTTO] [varchar] (5) NULL,
		[DES_PRODOTTO] [varchar] (100) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 