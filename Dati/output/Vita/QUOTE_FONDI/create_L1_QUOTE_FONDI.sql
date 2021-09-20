CREATE TABLE [L1].[T_Vita_QUOTE_FONDI]( 
		[COD_ABI] [varchar] (5) NOT NULL, 
		[COD_CONTRATTO] [varchar] (34) NOT NULL, 
		[COD_COMPAGNIA] [varchar] (5) NULL, 
		[COD_POLIZZA] [varchar] (11) NULL, 
		[COD_PRODOTTO] [varchar] (5) NULL, 
		[COD_TARIFFA_ENTE] [varchar] (5) NULL, 
		[COD_PRODOTTO_INTERNO] [varchar] (20) NULL, 
		[COD_GARANZIA] [varchar] (5) NULL, 
		[COD_FONDO] [varchar] (5) NOT NULL, 
		[DAT_AVVALORAMENTO] [date] NULL, 
		[NUM_QUOTE_OPERAZIONE] [int] NULL, 
		[IMP_CONTROVALORE_QUOTE] [numeric] (20, 6) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 CONSTRAINT [PK_L1_T_QUOTE_FONDI] PRIMARY KEY CLUSTERED 
(		[COD_ABI] ASC,
		[COD_CONTRATTO] ASC,
		[COD_FONDO] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] 