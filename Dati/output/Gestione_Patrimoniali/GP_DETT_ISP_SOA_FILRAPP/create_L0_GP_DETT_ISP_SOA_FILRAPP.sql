CREATE TABLE [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_SOA_FILRAPP]( 
		[COD_ABI] [varchar] (5) NULL,
		[COD_FILIALE_RAPPORTO] [varchar] (5) NULL,
		[DES_FILIALE_RAPPORTO] [varchar] (100) NULL,
		[DIREZIONE_REGIONALE] [varchar] (100) NULL,
		[COD_AREA] [varchar] (5) NULL,
		[DES_AREA] [varchar] (100) NULL,
		[NOM_INDIRIZZO_FILIALE_UFFICIO] [varchar] (100) NULL,
		[NOM_COMUNE_FILIALE_UFFICIO] [varchar] (100) NULL,
		[GEO_COORDINATA_X] [varchar] (17) NULL,
		[GEO_COORDINATA_Y] [varchar] (17) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 