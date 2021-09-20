CREATE TABLE [L1].[T_Vita_Area_Fondi]( 
		[COD_ABI] [varchar] (5) NOT NULL, 
		[COD_CONTRATTO] [varchar] (34) NOT NULL, 
		[COD_PRODOTTO] [varchar] (20) NULL, 
		[DES_PRODOTTO] [varchar] (100) NULL, 
		[DES_MACROAREA] [varchar] (34) NULL, 
		[COD_COMPONENTE] [varchar] (5) NULL, 
		[DES_COMPONENTE] [varchar] (100) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 CONSTRAINT [PK_L1_T_Area_Fondi] PRIMARY KEY CLUSTERED 
(		[COD_ABI] ASC,
		[COD_CONTRATTO] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] 