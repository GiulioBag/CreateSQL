CREATE TABLE [L1].[T_Vita_Fondi_Polizze]( 
		[COD_ABI] [varchar] (5) NOT NULL, 
		[COD_CONTRATTO] [varchar] (34) NOT NULL, 
		[COD_FONDO] [varchar] (5) NOT NULL, 
		[COD_COMPAGNIA] [varchar] (5) NOT NULL, 
		[DAT_EMISSIONE] [date] NULL, 
		[DATA_QUOTAZIONE] [date] NULL, 
		[IMP_QUOTA] [numeric] (20, 2) NULL, 
		[NUM_QUOTE_OPERAZIONE] [int] NULL, 
		[IMP_CONTROVALORE] [numeric] (20, 2) NULL, 
		[PERC_CONTROVALORE] [numeric] (20, 2) NULL, 
		[DAT_AVVALORAMENTO] [date] NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 CONSTRAINT [PK_L1_T_Fondi_Polizze] PRIMARY KEY CLUSTERED 
(		[COD_ABI] ASC,
		[COD_CONTRATTO] ASC,
		[COD_FONDO] ASC,
		[COD_COMPAGNIA] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] 