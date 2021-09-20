/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Anagrafica_Polizze] alla tabella [L1].[T_Vita_Anagrafica_Polizze].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Anagrafica_Polizze]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Anagrafica_Polizze]
	@Exec_Id [int]
WITH EXECUTE AS CALLER
AS
	SET LANGUAGE us_english;
	SET NOCOUNT ON
;
	DECLARE
		@ProcName	varchar(255) = CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)), N'.', QUOTENAME(OBJECT_NAME(@@PROCID)))
		,@Step	VARCHAR(500) =''
		,@Now	datetime = getdate()
		,@ID_Flusso	int
		,@maxDate	date = '99991231' --'12/31/9999'
	;
	
	BEGIN TRY 
 

		SET @Step = '1. Get delle informazioni dalla tabella [JOB].[T_Flusso_DataLoad]'
		;
		SELECT TOP 1
			@ID_Flusso = [ID_Flusso]
		FROM [JOB].[T_Flusso_DataLoad]
		WHERE [Exec_Id] = @Exec_Id
		;
		
		UPDATE [L0].[T_Vita_Anagrafica_Polizze]
		SET [BitMask_Scarti] = 0 
		; 
 

--		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
--		;
--		UPDATE [sn]
--		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--		FROM [L0].[T_Vita_Anagrafica_Polizze] [sn]
--		JOIN ( SELECT 
--			[sn].[COD_ABI]
--			,[sn].[COD_CONTRATTO]
--		FROM [L0].[T_Vita_Anagrafica_Polizze] [sn]
--		WHERE [Exec_Id] = @Exec_Id
--			[sn].[COD_ABI] <> '' AND [sn].[COD_ABI] IS NOT NULL
--			AND [sn].[COD_CONTRATTO] <> '' AND [sn].[COD_CONTRATTO] IS NOT NULL
--		GROUP BY
--			[sn].[COD_ABI]
--			,[sn].[COD_CONTRATTO]
--		HAVING COUNT(*) > 1
--	) [sn2]
--	on
--		[sn].[COD_ABI] = [sn2].[COD_ABI]
--		AND [sn].[COD_CONTRATTO] = [sn2].[COD_CONTRATTO]
--	CROSS APPLY [L0_SCARTI].[T_Desc_Scarti] scarti
--	WHERE [Exec_Id]=@Exec_Id
--		AND [scarti].[Cod_Scarto] = 'DUPLICATE_KEY' --Codice d'errore 
--		AND [scarti].[ID_Flusso] = @ID_Flusso
--		AND [scarti].[Flag_Enabled] = 1 
-- 
--
	SET @Step = '2.2 Scarti: Applicazione criterio di scarto EMPTY_KEY'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Polizze] [sn]
	CROSS JOIN[L0_SCARTI].[T_Desc_Scarti][scarti]
	where[Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_KEY' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			([sn].[COD_ABI] is null OR [sn].[COD_ABI] = '')
			OR ([sn].[COD_CONTRATTO] is null OR [sn].[COD_CONTRATTO] = '')
		)
	; 
 

	--Viene preso solamente una riga tra le N righe duplicate.
	--Viene presa la prima riga del file (Ordinament o per Row_Id ASC)
	SET @Step = '2.3 Scarti: Applicazione criterio di scarto GET_ONE_DUPLICATE_KEY'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Polizze] [sn]
	JOIN (
		SELECT 
			[r].[COD_ABI]
			,[r].[COD_CONTRATTO]
			,[r].[Row_Id]
			,[r].[Exec_Id]
			,ROW_NUMBER() OVER(
			PARTIOTION BY
					[r].[COD_ABI]
					,[r].[COD_CONTRATTO]
				ORDER BY
					[r].[Row_Id] ASC
					,[r].[Exec_Id] ASC]
		) as [rn]
		FROM [L0].[T_Vita_Anagrafica_Polizze] [r]
		WHERE [Exec_Id] = @Exec_Id
	) [sn2]
		on
			[sn].[COD_ABI] = [sn2].[COD_ABI]
			AND [sn].[COD_CONTRATTO] = [sn2].[COD_CONTRATTO]
			AND [sn].[Row_Id] = [sn2].[Row_Id]
			AND [sn].[Exec_Id] = [sn2].[Exec_Id]
	[sn].[COD_ABI] = [sn2].[COD_ABI]
	AND [sn].[COD_CONTRATTO] = [sn2].[COD_CONTRATTO]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	WHERE [sn].[Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'GET_ONE_DUPLICATE_KEY' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND [sn2].[rn] > 1
	; 
 

	--Esclusione Date Null
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto EMPTY_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Polizze] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_APERTURA_CONTRATTO] is null OR [sn].[DAT_APERTURA_CONTRATTO] = '')
			--OR ([sn].[DAT_CHIUSURA_CONTRATTO] is null OR [sn].[DAT_CHIUSURA_CONTRATTO] = '')
			--OR ([sn].[DAT_EMISSIONE] is null OR [sn].[DAT_EMISSIONE] = '')
			--OR ([sn].[TMS_DECORRENZA] is null OR [sn].[TMS_DECORRENZA] = '')
			--OR ([sn].[DAT_ANNULLO] is null OR [sn].[DAT_ANNULLO] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Polizze] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_APERTURA_CONTRATTO] is not null AND [sn].[DAT_APERTURA_CONTRATTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_APERTURA_CONTRATTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_CHIUSURA_CONTRATTO] is not null AND [sn].[DAT_CHIUSURA_CONTRATTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_CHIUSURA_CONTRATTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_EMISSIONE] is not null AND [sn].[DAT_EMISSIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_EMISSIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[TMS_DECORRENZA] is not null AND [sn].[TMS_DECORRENZA]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[TMS_DECORRENZA], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_ANNULLO] is not null AND [sn].[DAT_ANNULLO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_ANNULLO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Polizze] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[NUM_ANNO_MESE] is null )
			--OR ([sn].[FLG_FUORI_SEDE] is null )
			--OR ([sn].[IMP_PREMIO_LORDO] is null )
			--OR ([sn].[IMP_PREMIO_NETTO] is null )
			--OR ([sn].[IMP_IMPOSTE] is null )
			--OR ([sn].[SALDI] is null )
			--OR ([sn].[flag_altra_pol] is null )
			--OR ([sn].[IMP_CONTROVALORE_POLIZZA] is null )
		)
	; 
 

	--Esclusione numeric non Validi
	SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Polizze] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[NUM_ANNO_MESE] is NOT null AND TRY_CAST([NUM_ANNO_MESE] as int) IS  NULL))
			OR ([sn].[FLG_FUORI_SEDE] is NOT null AND TRY_CAST([FLG_FUORI_SEDE] as bit) IS  NULL))
			OR ([sn].[IMP_PREMIO_LORDO] is NOT null AND TRY_CAST(REPLACE([IMP_PREMIO_LORDO], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[IMP_PREMIO_NETTO] is NOT null AND TRY_CAST(REPLACE([IMP_PREMIO_NETTO], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[IMP_IMPOSTE] is NOT null AND TRY_CAST(REPLACE([IMP_IMPOSTE], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[SALDI] is NOT null AND TRY_CAST(REPLACE([SALDI], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[flag_altra_pol] is NOT null AND TRY_CAST([flag_altra_pol] as bit) IS  NULL))
			OR ([sn].[IMP_CONTROVALORE_POLIZZA] is NOT null AND TRY_CAST(REPLACE([IMP_CONTROVALORE_POLIZZA], ',', '.') as NUMERIC (22,2)) IS  NULL))
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Vita_Anagrafica_Polizze](
		[NUM_ANNO_MESE]
		,[COD_ABI]
		,[COD_NDG]
		,[COD_CONTRATTO]
		,[COD_CONTRATTO_STANDARD]
		,[COD_STATO_RAPPORTO]
		,[DES_STATO_RAPPORTO]
		,[COD_POLIZZA]
		,[COD_ABI_FIL]
		,[COD_FILIALE]
		,[COD_FILIALE_EMISSIONE]
		,[COD_FILIALE_RIFERIMENTO]
		,[COD_FILIALE_PORTAFOGLIO]
		,[COD_CANALE]
		,[FLG_FUORI_SEDE]
		,[COD_TIPOLOGIA_FUORI_SEDE]
		,[COD_PRODOTTO_INTERNO]
		,[COD_PRODOTTO]
		,[COD_COMPAGNIA]
		,[COD_TARIFFA_ENTE]
		,[IMP_PREMIO_LORDO]
		,[IMP_PREMIO_NETTO]
		,[IMP_IMPOSTE]
		,[DAT_APERTURA_CONTRATTO]
		,[DAT_CHIUSURA_CONTRATTO]
		,[DAT_EMISSIONE]
		,[TMS_DECORRENZA]
		,[DAT_ANNULLO]
		,[SALDI]
		,[COD_PROFILO_INVESTIT_EMIS]
		,[DES_PROFILO_INVESTIT_EMIS]
		,[COD_ESPERIENZA_INVESTIM_EMIS]
		,[DES_ESPERIENZA_INVESTIM_EMIS]
		,[flag_altra_pol]
		,[IMP_CONTROVALORE_POLIZZA]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[NUM_ANNO_MESE]
		,[COD_ABI]
		,[COD_NDG]
		,[COD_CONTRATTO]
		,[COD_CONTRATTO_STANDARD]
		,[COD_STATO_RAPPORTO]
		,[DES_STATO_RAPPORTO]
		,[COD_POLIZZA]
		,[COD_ABI_FIL]
		,[COD_FILIALE]
		,[COD_FILIALE_EMISSIONE]
		,[COD_FILIALE_RIFERIMENTO]
		,[COD_FILIALE_PORTAFOGLIO]
		,[COD_CANALE]
		,[FLG_FUORI_SEDE]
		,[COD_TIPOLOGIA_FUORI_SEDE]
		,[COD_PRODOTTO_INTERNO]
		,[COD_PRODOTTO]
		,[COD_COMPAGNIA]
		,[COD_TARIFFA_ENTE]
		,[IMP_PREMIO_LORDO]
		,[IMP_PREMIO_NETTO]
		,[IMP_IMPOSTE]
		,[DAT_APERTURA_CONTRATTO]
		,[DAT_CHIUSURA_CONTRATTO]
		,[DAT_EMISSIONE]
		,[TMS_DECORRENZA]
		,[DAT_ANNULLO]
		,[SALDI]
		,[COD_PROFILO_INVESTIT_EMIS]
		,[DES_PROFILO_INVESTIT_EMIS]
		,[COD_ESPERIENZA_INVESTIM_EMIS]
		,[DES_ESPERIENZA_INVESTIM_EMIS]
		,[flag_altra_pol]
		,[IMP_CONTROVALORE_POLIZZA]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Polizze]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Anagrafica_Polizze] AS dst
	USING 
		( SELECT
			TRY_CAST([NUM_ANNO_MESE] as int) 
			,[COD_ABI]
			,[COD_NDG]
			,[COD_CONTRATTO]
			,[COD_CONTRATTO_STANDARD]
			,[COD_STATO_RAPPORTO]
			,[DES_STATO_RAPPORTO]
			,[COD_POLIZZA]
			,[COD_ABI_FIL]
			,[COD_FILIALE]
			,[COD_FILIALE_EMISSIONE]
			,[COD_FILIALE_RIFERIMENTO]
			,[COD_FILIALE_PORTAFOGLIO]
			,[COD_CANALE]
			,TRY_CAST([FLG_FUORI_SEDE] as bit) 
			,[COD_TIPOLOGIA_FUORI_SEDE]
			,[COD_PRODOTTO_INTERNO]
			,[COD_PRODOTTO]
			,[COD_COMPAGNIA]
			,[COD_TARIFFA_ENTE]
			,TRY_CAST(REPLACE([IMP_PREMIO_LORDO], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([IMP_PREMIO_NETTO], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([IMP_IMPOSTE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CONVERT (date, stuff(stuff([DAT_APERTURA_CONTRATTO], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_CHIUSURA_CONTRATTO], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_EMISSIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([TMS_DECORRENZA], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_ANNULLO], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CAST(REPLACE([SALDI], ',', '.') as NUMERIC (22,2)) 
			,[COD_PROFILO_INVESTIT_EMIS]
			,[DES_PROFILO_INVESTIT_EMIS]
			,[COD_ESPERIENZA_INVESTIM_EMIS]
			,[DES_ESPERIENZA_INVESTIM_EMIS]
			,TRY_CAST([flag_altra_pol] as bit) 
			,TRY_CAST(REPLACE([IMP_CONTROVALORE_POLIZZA], ',', '.') as NUMERIC (22,2)) 
			,[Row_Id]
		FROM [L0].[T_Vita_Anagrafica_Polizze]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			on [src].[COD_ABI] = [dst].[COD_ABI]
			AND [src].[COD_CONTRATTO] = [dst].[COD_CONTRATTO]
	WHEN not matched THEN INSERT (
			[NUM_ANNO_MESE]
			,[COD_ABI]
			,[COD_NDG]
			,[COD_CONTRATTO]
			,[COD_CONTRATTO_STANDARD]
			,[COD_STATO_RAPPORTO]
			,[DES_STATO_RAPPORTO]
			,[COD_POLIZZA]
			,[COD_ABI_FIL]
			,[COD_FILIALE]
			,[COD_FILIALE_EMISSIONE]
			,[COD_FILIALE_RIFERIMENTO]
			,[COD_FILIALE_PORTAFOGLIO]
			,[COD_CANALE]
			,[FLG_FUORI_SEDE]
			,[COD_TIPOLOGIA_FUORI_SEDE]
			,[COD_PRODOTTO_INTERNO]
			,[COD_PRODOTTO]
			,[COD_COMPAGNIA]
			,[COD_TARIFFA_ENTE]
			,[IMP_PREMIO_LORDO]
			,[IMP_PREMIO_NETTO]
			,[IMP_IMPOSTE]
			,[DAT_APERTURA_CONTRATTO]
			,[DAT_CHIUSURA_CONTRATTO]
			,[DAT_EMISSIONE]
			,[TMS_DECORRENZA]
			,[DAT_ANNULLO]
			,[SALDI]
			,[COD_PROFILO_INVESTIT_EMIS]
			,[DES_PROFILO_INVESTIT_EMIS]
			,[COD_ESPERIENZA_INVESTIM_EMIS]
			,[DES_ESPERIENZA_INVESTIM_EMIS]
			,[flag_altra_pol]
			,[IMP_CONTROVALORE_POLIZZA]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[NUM_ANNO_MESE]
			,[src].[COD_ABI]
			,[src].[COD_NDG]
			,[src].[COD_CONTRATTO]
			,[src].[COD_CONTRATTO_STANDARD]
			,[src].[COD_STATO_RAPPORTO]
			,[src].[DES_STATO_RAPPORTO]
			,[src].[COD_POLIZZA]
			,[src].[COD_ABI_FIL]
			,[src].[COD_FILIALE]
			,[src].[COD_FILIALE_EMISSIONE]
			,[src].[COD_FILIALE_RIFERIMENTO]
			,[src].[COD_FILIALE_PORTAFOGLIO]
			,[src].[COD_CANALE]
			,[src].[FLG_FUORI_SEDE]
			,[src].[COD_TIPOLOGIA_FUORI_SEDE]
			,[src].[COD_PRODOTTO_INTERNO]
			,[src].[COD_PRODOTTO]
			,[src].[COD_COMPAGNIA]
			,[src].[COD_TARIFFA_ENTE]
			,[src].[IMP_PREMIO_LORDO]
			,[src].[IMP_PREMIO_NETTO]
			,[src].[IMP_IMPOSTE]
			,[src].[DAT_APERTURA_CONTRATTO]
			,[src].[DAT_CHIUSURA_CONTRATTO]
			,[src].[DAT_EMISSIONE]
			,[src].[TMS_DECORRENZA]
			,[src].[DAT_ANNULLO]
			,[src].[SALDI]
			,[src].[COD_PROFILO_INVESTIT_EMIS]
			,[src].[DES_PROFILO_INVESTIT_EMIS]
			,[src].[COD_ESPERIENZA_INVESTIM_EMIS]
			,[src].[DES_ESPERIENZA_INVESTIM_EMIS]
			,[src].[flag_altra_pol]
			,[src].[IMP_CONTROVALORE_POLIZZA]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[NUM_ANNO_MESE] = [src].[NUM_ANNO_MESE]
			,[COD_NDG] = [src].[COD_NDG]
			,[COD_CONTRATTO_STANDARD] = [src].[COD_CONTRATTO_STANDARD]
			,[COD_STATO_RAPPORTO] = [src].[COD_STATO_RAPPORTO]
			,[DES_STATO_RAPPORTO] = [src].[DES_STATO_RAPPORTO]
			,[COD_POLIZZA] = [src].[COD_POLIZZA]
			,[COD_ABI_FIL] = [src].[COD_ABI_FIL]
			,[COD_FILIALE] = [src].[COD_FILIALE]
			,[COD_FILIALE_EMISSIONE] = [src].[COD_FILIALE_EMISSIONE]
			,[COD_FILIALE_RIFERIMENTO] = [src].[COD_FILIALE_RIFERIMENTO]
			,[COD_FILIALE_PORTAFOGLIO] = [src].[COD_FILIALE_PORTAFOGLIO]
			,[COD_CANALE] = [src].[COD_CANALE]
			,[FLG_FUORI_SEDE] = [src].[FLG_FUORI_SEDE]
			,[COD_TIPOLOGIA_FUORI_SEDE] = [src].[COD_TIPOLOGIA_FUORI_SEDE]
			,[COD_PRODOTTO_INTERNO] = [src].[COD_PRODOTTO_INTERNO]
			,[COD_PRODOTTO] = [src].[COD_PRODOTTO]
			,[COD_COMPAGNIA] = [src].[COD_COMPAGNIA]
			,[COD_TARIFFA_ENTE] = [src].[COD_TARIFFA_ENTE]
			,[IMP_PREMIO_LORDO] = [src].[IMP_PREMIO_LORDO]
			,[IMP_PREMIO_NETTO] = [src].[IMP_PREMIO_NETTO]
			,[IMP_IMPOSTE] = [src].[IMP_IMPOSTE]
			,[DAT_APERTURA_CONTRATTO] = [src].[DAT_APERTURA_CONTRATTO]
			,[DAT_CHIUSURA_CONTRATTO] = [src].[DAT_CHIUSURA_CONTRATTO]
			,[DAT_EMISSIONE] = [src].[DAT_EMISSIONE]
			,[TMS_DECORRENZA] = [src].[TMS_DECORRENZA]
			,[DAT_ANNULLO] = [src].[DAT_ANNULLO]
			,[SALDI] = [src].[SALDI]
			,[COD_PROFILO_INVESTIT_EMIS] = [src].[COD_PROFILO_INVESTIT_EMIS]
			,[DES_PROFILO_INVESTIT_EMIS] = [src].[DES_PROFILO_INVESTIT_EMIS]
			,[COD_ESPERIENZA_INVESTIM_EMIS] = [src].[COD_ESPERIENZA_INVESTIM_EMIS]
			,[DES_ESPERIENZA_INVESTIM_EMIS] = [src].[DES_ESPERIENZA_INVESTIM_EMIS]
			,[flag_altra_pol] = [src].[flag_altra_pol]
			,[IMP_CONTROVALORE_POLIZZA] = [src].[IMP_CONTROVALORE_POLIZZA]
			,[Exec_Id_UpdatedOn] = @Exec_Id
			,[DateTime_UpdatedOn] = @Now
			,[Row_Id_UpdatedOn]  = [src].[Row_Id]
	;
	COMMIT TRANSACTION


	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END
		;
		DECLARE @Message VARCHAR(MAX) = 'STEP ' + @step + ' ____'+ ERROR_MESSAGE() + '____ '
		;
		RAISERROR (@Message, 16,1)
		;
		
	END CATCH