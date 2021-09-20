CREATE VIEW [L1].[V_Vita_Anagrafica_Polizze]	 AS
	SELECT 
		[r].[NUM_ANNO_MESE]
		,[r].[COD_ABI]
		,[r].[COD_NDG]
		,[r].[COD_CONTRATTO]
		,[r].[COD_CONTRATTO_STANDARD]
		,[r].[COD_STATO_RAPPORTO]
		,[r].[DES_STATO_RAPPORTO]
		,[r].[COD_POLIZZA]
		,[r].[COD_ABI_FIL]
		,[r].[COD_FILIALE]
		,[r].[COD_FILIALE_EMISSIONE]
		,[r].[COD_FILIALE_RIFERIMENTO]
		,[r].[COD_FILIALE_PORTAFOGLIO]
		,[r].[COD_CANALE]
		,[r].[FLG_FUORI_SEDE]
		,[r].[COD_TIPOLOGIA_FUORI_SEDE]
		,[r].[COD_PRODOTTO_INTERNO]
		,[r].[COD_PRODOTTO]
		,[r].[COD_COMPAGNIA]
		,[r].[COD_TARIFFA_ENTE]
		,[r].[IMP_PREMIO_LORDO]
		,[r].[IMP_PREMIO_NETTO]
		,[r].[IMP_IMPOSTE]
		,[r].[DAT_APERTURA_CONTRATTO]
		,[r].[DAT_CHIUSURA_CONTRATTO]
		,[r].[DAT_EMISSIONE]
		,[r].[TMS_DECORRENZA]
		,[r].[DAT_ANNULLO]
		,[r].[SALDI]
		,[r].[COD_PROFILO_INVESTIT_EMIS]
		,[r].[DES_PROFILO_INVESTIT_EMIS]
		,[r].[COD_ESPERIENZA_INVESTIM_EMIS]
		,[r].[DES_ESPERIENZA_INVESTIM_EMIS]
		,[r].[flag_altra_pol]
		,[r].[IMP_CONTROVALORE_POLIZZA]

	FROM [L1].[T_Vita_Anagrafica_Polizze] as [r] 