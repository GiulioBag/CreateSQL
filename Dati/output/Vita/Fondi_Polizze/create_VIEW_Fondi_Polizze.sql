CREATE VIEW [L1].[V_Vita_Fondi_Polizze]	 AS
	SELECT 
		[r].[COD_ABI]
		,[r].[COD_CONTRATTO]
		,[r].[COD_FONDO]
		,[r].[COD_COMPAGNIA]
		,[r].[DAT_EMISSIONE]
		,[r].[DATA_QUOTAZIONE]
		,[r].[IMP_QUOTA]
		,[r].[NUM_QUOTE_OPERAZIONE]
		,[r].[IMP_CONTROVALORE]
		,[r].[PERC_CONTROVALORE]
		,[r].[DAT_AVVALORAMENTO]

	FROM [L1].[T_Vita_Fondi_Polizze] as [r] 