CREATE VIEW [L1].[V_Vita_Sospensioni]	 AS
	SELECT 
		[r].[COD_ABI]
		,[r].[COD_CONTRATTO]
		,[r].[COD_RICHIEDENTE]
		,[r].[DES_RICHIEDENTE]
		,[r].[DAT_INIZIO_SOSPENSIONE]
		,[r].[DAT_FINE_SOSPENSIONE]
		,[r].[COD_OPERAZIONE]
		,[r].[DES_OPERAZIONE]
		,[r].[DAT_OPERAZIONE]
		,[r].[COD_ESITO_ADEGUATEZZA]
		,[r].[DES_ESITO_ADEGUATEZZA]

	FROM [L1].[T_Vita_Sospensioni] as [r] 