CREATE TABLE [L1].[T_Vita_Reclami]( 
		[RECLAMO_RICORSO] [int] NULL, 
		[COD_ABI_RECLAMATO] [varchar] (5) NOT NULL, 
		[COD_FILIALE_RECLAMATA] [varchar] (5) NOT NULL, 
		[COD_PROTOCOLLO] [varchar] (10) NULL, 
		[COD_SOGGETTO_GIURIDICO] [varchar] (2) NULL, 
		[COD_TIPO_RECLAMO] [varchar] (3) NULL, 
		[DES_TIPO_RECLAMO] [varchar] (100) NULL, 
		[COD_TIPO_RISCONTRO] [varchar] (10) NULL, 
		[DES_TIPO_RISCONTRO] [varchar] (100) NULL, 
		[COD_SOGGETTO_COINVOLTO_RECLAMO] [varchar] (10) NULL, 
		[DES_SOGGETTO_COINVOLTO_RECLAMO] [varchar] (100) NULL, 
		[DAT_CENSIMENTO] [date] NULL, 
		[DAT_DECORRENZA] [date] NULL, 
		[COD_ESITO_RECLAMO] [varchar] (3) NULL, 
		[DAT_ARCHIVIAZIONE] [date] NULL, 
		[DES_ESITO_RECLAMO] [varchar] (50) NULL, 
		[COD_STATO_RECLAMO] [varchar] (1) NULL, 
		[DES_STATO_RECLAMO] [varchar] (35) NULL, 
		[COD_PRODOTTO] [varchar] (10) NULL, 
		[DES_PRODOTTO] [varchar] (100) NULL, 
		[COD_SUBPRODOTTO] [varchar] (10) NULL, 
		[DES_SUBPRODOTTO] [varchar] (100) NULL, 
		[COD_MACRO_SETTORE] [varchar] (10) NULL, 
		[DES_MACRO_SETTORE] [varchar] (40) NULL, 
		[COD_SUBMOTIVO] [varchar] (10) NULL, 
		[DES_SUBMOTIVO] [varchar] (220) NULL, 
		[COD_MOTIVO] [varchar] (10) NULL, 
		[DES_MOTIVO] [varchar] (40) NULL, 
		[COD_TIPO_CLIENTE] [varchar] (10) NULL, 
		[DES_TIPO_CLIENTE] [varchar] (10) NULL, 
		[COD_MITTENTE] [varchar] (10) NULL, 
		[COD_NDG] [varchar] (13) NOT NULL, 
		[IMP_RICHIESTO] [numeric] (20, 2) NULL, 
		[COD_TIPO_SUBRECLAMO] [varchar] (3) NULL, 
		[DES_SUBTIPO_RECLAMO] [varchar] (10) NULL, 
		[COD_CARATTERISTICA_RECLAMO] [varchar] (10) NULL, 
		[COD_STATO_RICORSO] [varchar] (10) NULL, 
		[COD_ESITO_RICORSO] [varchar] (10) NULL, 
		[DES_ESITO_RICORSO] [varchar] (40) NULL, 
		[DES_STATO_RICORSO] [varchar] (100) NULL, 
		[DES_ARGOMENTO] [varchar] (200) NULL, 
		[DES_NOTE] [varchar] (500) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 CONSTRAINT [PK_L1_T_Reclami] PRIMARY KEY CLUSTERED 
(		[COD_ABI_RECLAMATO] ASC,
		[COD_FILIALE_RECLAMATA] ASC,
		[COD_NDG] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] 