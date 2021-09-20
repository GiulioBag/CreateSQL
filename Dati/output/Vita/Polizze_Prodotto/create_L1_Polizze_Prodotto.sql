CREATE TABLE [L1].[T_Vita_Polizze_Prodotto]( 
		[COD_ABI] [varchar] (5) NOT NULL, 
		[COD_COMPAGNIA] [varchar] (5) NOT NULL, 
		[COD_NATURA] [varchar] (3) NULL, 
		[COD_RAMO_ASSICURATIVO] [varchar] (5) NULL, 
		[COD_TARIFFA_ENTE] [varchar] (5) NULL, 
		[COD_PRODOTTO_INTERNO] [varchar] (5) NOT NULL, 
		[COD_PRODOTTO] [varchar] (5) NOT NULL, 
		[DES_PRODOTTO] [varchar] (100) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 CONSTRAINT [PK_L1_T_Polizze_Prodotto] PRIMARY KEY CLUSTERED 
(		[COD_ABI] ASC,
		[COD_COMPAGNIA] ASC,
		[COD_PRODOTTO_INTERNO] ASC,
		[COD_PRODOTTO] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] 