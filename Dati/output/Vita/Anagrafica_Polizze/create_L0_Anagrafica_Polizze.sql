CREATE TABLE [L0].[T_Vita_Anagrafica_Polizze]( 
		[NUM_ANNO_MESE] [varchar] (8) NULL,
		[COD_ABI] [varchar] (5) NULL,
		[COD_NDG] [varchar] (13) NULL,
		[COD_CONTRATTO] [varchar] (34) NULL,
		[COD_CONTRATTO_STANDARD] [varchar] (34) NULL,
		[COD_STATO_RAPPORTO] [varchar] (1) NULL,
		[DES_STATO_RAPPORTO] [varchar] (100) NULL,
		[COD_POLIZZA] [varchar] (11) NULL,
		[COD_ABI_FIL] [varchar] (5) NULL,
		[COD_FILIALE] [varchar] (5) NULL,
		[COD_FILIALE_EMISSIONE] [varchar] (5) NULL,
		[COD_FILIALE_RIFERIMENTO] [varchar] (5) NULL,
		[COD_FILIALE_PORTAFOGLIO] [varchar] (5) NULL,
		[COD_CANALE] [varchar] (3) NULL,
		[FLG_FUORI_SEDE] [varchar] (8) NULL,
		[COD_TIPOLOGIA_FUORI_SEDE] [varchar] (1) NULL,
		[COD_PRODOTTO_INTERNO] [varchar] (20) NULL,
		[COD_PRODOTTO] [varchar] (5) NULL,
		[COD_COMPAGNIA] [varchar] (5) NULL,
		[COD_TARIFFA_ENTE] [varchar] (5) NULL,
		[IMP_PREMIO_LORDO] [varchar] (22) NULL,
		[IMP_PREMIO_NETTO] [varchar] (22) NULL,
		[IMP_IMPOSTE] [varchar] (22) NULL,
		[DAT_APERTURA_CONTRATTO] [varchar] (10) NULL,
		[DAT_CHIUSURA_CONTRATTO] [varchar] (10) NULL,
		[DAT_EMISSIONE] [varchar] (10) NULL,
		[TMS_DECORRENZA] [varchar] (10) NULL,
		[DAT_ANNULLO] [varchar] (10) NULL,
		[SALDI] [varchar] (22) NULL,
		[COD_PROFILO_INVESTIT_EMIS] [varchar] (20) NULL,
		[DES_PROFILO_INVESTIT_EMIS] [varchar] (100) NULL,
		[COD_ESPERIENZA_INVESTIM_EMIS] [varchar] (20) NULL,
		[DES_ESPERIENZA_INVESTIM_EMIS] [varchar] (100) NULL,
		[flag_altra_pol] [varchar] (8) NULL,
		[IMP_CONTROVALORE_POLIZZA] [varchar] (22) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 