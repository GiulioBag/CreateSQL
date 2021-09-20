CREATE VIEW [L1].[V_Vita_Polizze_Prodotto]	 AS
	SELECT 
		[r].[COD_ABI]
		,[r].[COD_COMPAGNIA]
		,[r].[COD_NATURA]
		,[r].[COD_RAMO_ASSICURATIVO]
		,[r].[COD_TARIFFA_ENTE]
		,[r].[COD_PRODOTTO_INTERNO]
		,[r].[COD_PRODOTTO]
		,[r].[DES_PRODOTTO]

	FROM [L1].[T_Vita_Polizze_Prodotto] as [r] 