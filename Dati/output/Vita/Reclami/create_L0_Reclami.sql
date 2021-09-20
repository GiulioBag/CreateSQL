CREATE TABLE [L0].[T_Vita_Reclami]( 
		[RECLAMO_RICORSO] [varchar] (8) NULL,
		[COD_ABI_RECLAMATO] [varchar] (5) NULL,
		[COD_FILIALE_RECLAMATA] [varchar] (5) NULL,
		[COD_PROTOCOLLO] [varchar] (10) NULL,
		[COD_SOGGETTO_GIURIDICO] [varchar] (2) NULL,
		[COD_TIPO_RECLAMO] [varchar] (3) NULL,
		[DES_TIPO_RECLAMO] [varchar] (100) NULL,
		[COD_TIPO_RISCONTRO] [varchar] (10) NULL,
		[DES_TIPO_RISCONTRO] [varchar] (100) NULL,
		[COD_SOGGETTO_COINVOLTO_RECLAMO] [varchar] (10) NULL,
		[DES_SOGGETTO_COINVOLTO_RECLAMO] [varchar] (100) NULL,
		[DAT_CENSIMENTO] [varchar] (10) NULL,
		[DAT_DECORRENZA] [varchar] (10) NULL,
		[COD_ESITO_RECLAMO] [varchar] (3) NULL,
		[DAT_ARCHIVIAZIONE] [varchar] (10) NULL,
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
		[COD_NDG] [varchar] (13) NULL,
		[IMP_RICHIESTO] [varchar] (22) NULL,
		[COD_TIPO_SUBRECLAMO] [varchar] (3) NULL,
		[DES_SUBTIPO_RECLAMO] [varchar] (10) NULL,
		[COD_CARATTERISTICA_RECLAMO] [varchar] (10) NULL,
		[COD_STATO_RICORSO] [varchar] (10) NULL,
		[COD_ESITO_RICORSO] [varchar] (10) NULL,
		[DES_ESITO_RICORSO] [varchar] (40) NULL,
		[DES_STATO_RICORSO] [varchar] (100) NULL,
		[DES_ARGOMENTO] [varchar] (200) NULL,
		[DES_NOTE] [varchar] (500) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 