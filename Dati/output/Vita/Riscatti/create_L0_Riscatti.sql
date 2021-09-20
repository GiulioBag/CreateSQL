CREATE TABLE [L0].[T_Vita_Riscatti]( 
		[COD_ABI] [varchar] (5) NULL,
		[COD_CONTRATTO] [varchar] (34) NULL,
		[PRO_VERSAMENTO] [varchar] (8) NULL,
		[COD_TIPO_LIQUIDIZIONE] [varchar] (1) NULL,
		[DES_TIPO_LIQUIDAZIONE] [varchar] (100) NULL,
		[COD_CAUSALE_OPERAZIONE] [varchar] (2) NULL,
		[COD_STATO_OPERAZIONE] [varchar] (1) NULL,
		[COD_FISCALE_BENEFICIARIO] [varchar] (16) NULL,
		[COD_CANALE_EVENTO] [varchar] (3) NULL,
		[COD_ABI_INSERIMENTO_MOVIMENTO] [varchar] (5) NULL,
		[COD_UO_INSERIMENTO_MOVIMENTO] [varchar] (5) NULL,
		[COD_OPERAZIONE] [varchar] (5) NULL,
		[COD_ESITO_ADEGUATEZZA] [varchar] (2) NULL,
		[COD_ADEGUATEZZA_ENTE] [varchar] (1) NULL,
		[DAT_ORDINE] [varchar] (10) NULL,
		[DAT_RICHIESTA] [varchar] (10) NULL,
		[DAT_EFFETTO] [varchar] (10) NULL,
		[DAT_EFFETTIVA] [varchar] (10) NULL,
		[DAT_VALUTA_ACCREDITO] [varchar] (10) NULL,
		[IMP_PAGAMENTO] [varchar] (22) NULL,
		[IMP_SPESE] [varchar] (22) NULL,
		[IMP_NETTO_OPERAZIONE] [varchar] (22) NULL,
		[IMP_TASSAZIONE] [varchar] (22) NULL,
		[IMP_VOLATILITA_PTF_MODIFICATO] [varchar] (22) NULL,
		[IMP_VOLATILITA_PTF_REALE] [varchar] (22) NULL,
		[IMP_VOLATILITA_PTF_IDEALE] [varchar] (22) NULL,
		[NUM_SOGLIA] [varchar] (8) NULL,
		[IMP_SOGLIA] [varchar] (22) NULL,
		[PRC_SOGLIA_CONCTRZ_RAGGIUNTA] [varchar] (22) NULL,
		[PRC_SOGLIA_CONCENTRAZIONE] [varchar] (22) NULL,
		[DAT_INSERIMENTO_ORDINE] [varchar] (10) NULL,
		[DAT_INSERIMENTO_MOVIMENTO] [varchar] (10) NULL,
		[Row_Id] [int] IDENTITY(1,1) NOT NULL, 
		[Exec_Id] [int] NULL,
		[Bitmask_Scarti] [bigint] NULL
) ON [PRIMARY] 