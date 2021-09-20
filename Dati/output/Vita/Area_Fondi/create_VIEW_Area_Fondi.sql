CREATE VIEW [L1].[V_Vita_Area_Fondi]	 AS
	SELECT 
		[r].[COD_ABI]
		,[r].[COD_CONTRATTO]
		,[r].[COD_PRODOTTO]
		,[r].[DES_PRODOTTO]
		,[r].[DES_MACROAREA]
		,[r].[COD_COMPONENTE]
		,[r].[DES_COMPONENTE]

	FROM [L1].[T_Vita_Area_Fondi] as [r] 