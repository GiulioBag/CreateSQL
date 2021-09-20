CREATE TABLE [L0].[T_Vita_Sospensioni]( 
		[COD_ABI] [varchar] (5) NULL,
		[COD_CONTRATTO] [varchar] (34) NULL,
		[COD_RICHIEDENTE] [varchar] (1) NULL,
		[DES_RICHIEDENTE] [varchar] (100) NULL,
		[DAT_INIZIO_SOSPENSIONE] [varchar] (20) NULL,
		[DAT_FINE_SOSPENSIONE] [varchar] (20) NULL,
		[COD_OPERAZIONE] [varchar] (5) NULL,
		[DES_OPERAZIONE] [varchar] (100) NULL,
		[DAT_OPERAZIONE] [varchar] (20) NULL,
		[COD_ESITO_ADEGUATEZZA] [varchar] (2) NULL,
		[DES_ESITO_ADEGUATEZZA] [varchar] (100) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 