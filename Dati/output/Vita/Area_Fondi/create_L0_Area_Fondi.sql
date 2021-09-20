CREATE TABLE [L0].[T_Vita_Area_Fondi]( 
		[COD_ABI] [varchar] (5) NULL,
		[COD_CONTRATTO] [varchar] (34) NULL,
		[COD_PRODOTTO] [varchar] (20) NULL,
		[DES_PRODOTTO] [varchar] (100) NULL,
		[DES_MACROAREA] [varchar] (34) NULL,
		[COD_COMPONENTE] [varchar] (5) NULL,
		[DES_COMPONENTE] [varchar] (100) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 