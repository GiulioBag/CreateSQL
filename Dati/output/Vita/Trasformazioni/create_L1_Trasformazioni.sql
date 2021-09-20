CREATE TABLE [L1].[T_Vita_Trasformazioni]( 
		[COD_ABI] [varchar] (5) NOT NULL, 
		[COD_NDG_CONTRATTUALE] [varchar] (13) NOT NULL, 
		[COD_CONTRATTO_RISC] [varchar] (34) NULL, 
		[COD_POLIZZA_RISC] [varchar] (11) NULL, 
		[COD_COMPAGNIA_RISC] [varchar] (5) NULL, 
		[COD_TARIFFA_ENTE_RISC] [varchar] (5) NULL, 
		[COD_PRODOTTO_INTERNO_RISC] [varchar] (20) NULL, 
		[DES_PRODOTTO_RISC] [varchar] (100) NULL, 
		[COD_TIPO_LIQUIDIZIONE_RISC] [varchar] (1) NULL, 
		[DES_TIPO_LIQUIDAZIONE] [varchar] (100) NULL, 
		[DAT_RISCATTO] [date] NULL, 
		[COD_CONTRATTO_EMIS] [varchar] (34) NULL, 
		[COD_POLIZZA_EMIS] [varchar] (11) NULL, 
		[COD_COMPAGNIA_EMIS] [varchar] (5) NULL, 
		[COD_TARIFFA_ENTE_EMIS] [varchar] (5) NULL, 
		[COD_PRODOTTO_INTERNO_EMIS] [varchar] (20) NULL, 
		[DES_PRODOTTO] [varchar] (100) NULL, 
		[DAT_EMISSIONE] [date] NULL, 
		[trasf] [varchar] (5) NULL, 
		[IMP_EMISSIONE] [numeric] (20, 2) NULL, 
		[IMP_RISCATTO] [numeric] (20, 2) NULL, 
		[CONTROVALORE] [None] NULL, 
		[DATA_TRASFORMAZIONE] [date] NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 CONSTRAINT [PK_L1_T_Trasformazioni] PRIMARY KEY CLUSTERED 
(		[COD_ABI] ASC,
		[COD_NDG_CONTRATTUALE] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] 