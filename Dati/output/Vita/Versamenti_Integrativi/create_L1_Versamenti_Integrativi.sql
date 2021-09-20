CREATE TABLE [L1].[T_Vita_Versamenti_Integrativi]( 
		[COD_ABI] [varchar] (5) NOT NULL, 
		[COD_CONTRATTO] [varchar] (34) NOT NULL, 
		[COD_ID_TRANSAZIONE] [varchar] (50) NULL, 
		[COD_ESITO_ADEGUATEZZA] [varchar] (2) NULL, 
		[COD_ADEGUATEZZA_ENTE] [varchar] (1) NULL, 
		[COD_ADEGUATEZZA_OPERATORE] [varchar] (1) NULL, 
		[DAT_RICHIESTA] [date] NULL, 
		[DAT_MATURAZIONE] [date] NULL, 
		[DAT_EFFETTO] [date] NULL, 
		[COD_TIPO_RATA] [varchar] (1) NULL, 
		[IMP_VERSAMENTO] [numeric] (20, 2) NULL, 
		[IMP_VERSAMENTO_NETTO] [numeric] (20, 2) NULL, 
		[IMP_VERSAMENTO_INVESTITO] [numeric] (20, 2) NULL, 
		[IMP_IMPOSTE] [numeric] (20, 2) NULL, 
		[IMP_VOLATILITA_PTF_MODIFICATO] [numeric] (20, 2) NULL, 
		[IMP_VOLATILITA_PTF_REALE] [numeric] (20, 2) NULL, 
		[IMP_VOLATILITA_PTF_IDEALE] [numeric] (20, 2) NULL, 
		[NUM_SOGLIA] [int] NULL, 
		[IMP_SOGLIA] [numeric] (20, 2) NULL, 
		[PRC_SOGLIA_CONCTRZ_RAGGIUNTA] [numeric] (20, 2) NULL, 
		[PRC_SOGLIA_CONCENTRAZIONE] [numeric] (20, 2) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 CONSTRAINT [PK_L1_T_Versamenti_Integrativi] PRIMARY KEY CLUSTERED 
(		[COD_ABI] ASC,
		[COD_CONTRATTO] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] 