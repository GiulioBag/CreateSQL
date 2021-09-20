/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] alla tabella [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI]
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
		
		UPDATE [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI]
		SET [BitMask_Scarti] = 0 
		; 
 

----		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
----		;
----		UPDATE [sn]
----		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
----		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] [sn]
----		JOIN ( SELECT 
----			Inserire qui le eventuali chiavi nella forma [sn].[key]
----		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] [sn]
----		WHERE [Exec_Id] = @Exec_Id
----			Inserire qui le eventuali chiavi nella forma [sn].[key] <> '' AND [sn].[key] is not null
----		GROUP BY
----			Inserire qui le eventuali chiavi nella forma [sn].[key]
----		HAVING COUNT(*) > 1
----	) [sn2]
----	on
----		Inserire qui le eventuali chiavi nella forma [sn].[key] = [sn2].[key
----	CROSS APPLY [L0_SCARTI].[T_Desc_Scarti] scarti
----	WHERE [Exec_Id]=@Exec_Id
----		AND [scarti].[Cod_Scarto] = 'DUPLICATE_KEY' --Codice d'errore 
----		AND [scarti].[ID_Flusso] = @ID_Flusso
----		AND [scarti].[Flag_Enabled] = 1 
---- 
----
--	SET @Step = '2.2 Scarti: Applicazione criterio di scarto EMPTY_KEY'
--	;
--	UPDATE [sn]
--	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] [sn]
--	CROSS JOIN[L0_SCARTI].[T_Desc_Scarti][scarti]
--	where[Exec_Id] = @Exec_Id
--		AND [scarti].[Cod_Scarto] = 'EMPTY_KEY' --Codice d'errore
--		AND [scarti].[ID_Flusso] = @ID_Flusso
--		AND [scarti].[Flag_Enabled] = 1
--		AND (
--			Inserire qui le chiavi nella forma OR ([sn].[key] is null OR [sn].[key] = '')
--		)
--	; 
-- 
--
--	--Viene preso solamente una riga tra le N righe duplicate.
--	--Viene presa la prima riga del file (Ordinament o per Row_Id ASC)
--	SET @Step = '2.3 Scarti: Applicazione criterio di scarto GET_ONE_DUPLICATE_KEY'
--	;
--	UPDATE [sn]
--	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] [sn]
--	JOIN (
--		SELECT 
--			Inserire qui le chiavi nella forma [r].[key]
--			,[r].[Row_Id]
--			,[r].[Exec_Id]
--			,ROW_NUMBER() OVER(
--			PARTIOTION BY
--					Inserire qui le chiavi nella forma [r].[key]
--				ORDER BY
--					[r].[Row_Id] ASC
--					,[r].[Exec_Id] ASC]
--		) as [rn]
--		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] [r]
--		WHERE [Exec_Id] = @Exec_Id
--	) [sn2]
--		on
--			Inserire qui le chiavi nella forma: AND [sn].[key] = [sn2].[key]
--			AND [sn].[Row_Id] = [sn2].[Row_Id]
--			AND [sn].[Exec_Id] = [sn2].[Exec_Id]
--	Inserire qui le chiavi nella forma: AND [sn].[key] = [sn2].[key]
--	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
--	WHERE [sn].[Exec_Id] = @Exec_Id
--		AND [scarti].[Cod_Scarto] = 'GET_ONE_DUPLICATE_KEY' --Codice d'errore
--		AND [scarti].[ID_Flusso] = @ID_Flusso
--		AND [scarti].[Flag_Enabled] = 1
--		AND [sn2].[rn] > 1
--	; 
-- 
--
	--Esclusione Date Null
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto EMPTY_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_INSERIMENTO_ORDINE] is null OR [sn].[DAT_INSERIMENTO_ORDINE] = '')
			--OR ([sn].[DAT_MOVIMENTO] is null OR [sn].[DAT_MOVIMENTO] = '')
			--OR ([sn].[DAT_VALUTA_MOVIMENTO] is null OR [sn].[DAT_VALUTA_MOVIMENTO] = '')
			--OR ([sn].[PROFILO_DAT_COMPILAZIONE] is null OR [sn].[PROFILO_DAT_COMPILAZIONE] = '')
			--OR ([sn].[CONESPR_DAT_COMPILAZIONE] is null OR [sn].[CONESPR_DAT_COMPILAZIONE] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_INSERIMENTO_ORDINE] is not null AND [sn].[DAT_INSERIMENTO_ORDINE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_INSERIMENTO_ORDINE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_MOVIMENTO] is not null AND [sn].[DAT_MOVIMENTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_MOVIMENTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_VALUTA_MOVIMENTO] is not null AND [sn].[DAT_VALUTA_MOVIMENTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_VALUTA_MOVIMENTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[PROFILO_DAT_COMPILAZIONE] is not null AND [sn].[PROFILO_DAT_COMPILAZIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[PROFILO_DAT_COMPILAZIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[CONESPR_DAT_COMPILAZIONE] is not null AND [sn].[CONESPR_DAT_COMPILAZIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[CONESPR_DAT_COMPILAZIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[PRO_MOVIMENTO] is null )
			--OR ([sn].[IMP_MOVIMENTO] is null )
			--OR ([sn].[PRO_OPERAZIONE_DYNG] is null )
			--OR ([sn].[PRO_OPERAZIONE_SGR] is null )
		)
	; 
 

	--Esclusione numeric non Validi
	SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[PRO_MOVIMENTO] is NOT null AND TRY_CAST([PRO_MOVIMENTO] as int) IS  NULL))
			OR ([sn].[IMP_MOVIMENTO] is NOT null AND TRY_CAST(REPLACE([IMP_MOVIMENTO], ',', '.') as NUMERIC (23,3)) IS  NULL))
			OR ([sn].[PRO_OPERAZIONE_DYNG] is NOT null AND TRY_CAST([PRO_OPERAZIONE_DYNG] as int) IS  NULL))
			OR ([sn].[PRO_OPERAZIONE_SGR] is NOT null AND TRY_CAST([PRO_OPERAZIONE_SGR] as int) IS  NULL))
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI](
		[COD_ABI]
		,[COD_NDG_CONTRATTO]
		,[COD_CONTRATTO]
		,[COD_MANDATO]
		,[DAT_INSERIMENTO_ORDINE]
		,[DAT_MOVIMENTO]
		,[COD_CAUSALE_MOVIMENTO_GP]
		,[DES_CAUSALE_MOVIMENTO_GP]
		,[PRO_MOVIMENTO]
		,[DAT_VALUTA_MOVIMENTO]
		,[IMP_MOVIMENTO]
		,[COD_CONTRATTO_RIFERIMENTO]
		,[COD_ABI_KTO_RIFERIMENTO]
		,[COD_LINEA_PRODOTTO_RIFERIMENTO]
		,[DES_LINEA_PRODOTTO_RIFERIMENTO]
		,[PRO_OPERAZIONE_DYNG]
		,[PRO_OPERAZIONE_SGR]
		,[COD_STATO_MOVIMENTO]
		,[DES_STATO_MOVIMENTO]
		,[COD_CANALE_EVENTO]
		,[DES_CANALE_EVENTO]
		,[COD_UO_INSERIMENTO_EVENTO]
		,[COD_NDG_ORDINANTE]
		,[ORIGINE_MOVIMENTO]
		,[COD_CONFERMA_SIM]
		,[COD_TIPO_OPERAZIONE]
		,[PROFILO_DAT_COMPILAZIONE]
		,[PROFILO_COD_PROFILO_INVESTITORE]
		,[PROFILO_DES_PROFILO_INVESTITORE]
		,[PROFILO_COD_TIPO_PROFILO]
		,[PROFILO_DES_TIPO_PROFILO]
		,[PROFILO_COD_STATO_PROFILO]
		,[PROFILO_DES_STATO_PROFILO]
		,[CONESPR_DAT_COMPILAZIONE]
		,[CONESPR_COD_CONOSC_ESPER]
		,[CONESPR_DES_CONOSC_ESPER]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[COD_ABI]
		,[COD_NDG_CONTRATTO]
		,[COD_CONTRATTO]
		,[COD_MANDATO]
		,[DAT_INSERIMENTO_ORDINE]
		,[DAT_MOVIMENTO]
		,[COD_CAUSALE_MOVIMENTO_GP]
		,[DES_CAUSALE_MOVIMENTO_GP]
		,[PRO_MOVIMENTO]
		,[DAT_VALUTA_MOVIMENTO]
		,[IMP_MOVIMENTO]
		,[COD_CONTRATTO_RIFERIMENTO]
		,[COD_ABI_KTO_RIFERIMENTO]
		,[COD_LINEA_PRODOTTO_RIFERIMENTO]
		,[DES_LINEA_PRODOTTO_RIFERIMENTO]
		,[PRO_OPERAZIONE_DYNG]
		,[PRO_OPERAZIONE_SGR]
		,[COD_STATO_MOVIMENTO]
		,[DES_STATO_MOVIMENTO]
		,[COD_CANALE_EVENTO]
		,[DES_CANALE_EVENTO]
		,[COD_UO_INSERIMENTO_EVENTO]
		,[COD_NDG_ORDINANTE]
		,[ORIGINE_MOVIMENTO]
		,[COD_CONFERMA_SIM]
		,[COD_TIPO_OPERAZIONE]
		,[PROFILO_DAT_COMPILAZIONE]
		,[PROFILO_COD_PROFILO_INVESTITORE]
		,[PROFILO_DES_PROFILO_INVESTITORE]
		,[PROFILO_COD_TIPO_PROFILO]
		,[PROFILO_DES_TIPO_PROFILO]
		,[PROFILO_COD_STATO_PROFILO]
		,[PROFILO_DES_STATO_PROFILO]
		,[CONESPR_DAT_COMPILAZIONE]
		,[CONESPR_COD_CONOSC_ESPER]
		,[CONESPR_DES_CONOSC_ESPER]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI] AS dst
	USING 
		( SELECT
			[COD_ABI]
			,[COD_NDG_CONTRATTO]
			,[COD_CONTRATTO]
			,[COD_MANDATO]
			,TRY_CONVERT (date, stuff(stuff([DAT_INSERIMENTO_ORDINE], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_MOVIMENTO], 6, 0, ' '), 3, 0, ' '), 106)
			,[COD_CAUSALE_MOVIMENTO_GP]
			,[DES_CAUSALE_MOVIMENTO_GP]
			,TRY_CAST([PRO_MOVIMENTO] as int) 
			,TRY_CONVERT (date, stuff(stuff([DAT_VALUTA_MOVIMENTO], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CAST(REPLACE([IMP_MOVIMENTO], ',', '.') as NUMERIC (23,3)) 
			,[COD_CONTRATTO_RIFERIMENTO]
			,[COD_ABI_KTO_RIFERIMENTO]
			,[COD_LINEA_PRODOTTO_RIFERIMENTO]
			,[DES_LINEA_PRODOTTO_RIFERIMENTO]
			,TRY_CAST([PRO_OPERAZIONE_DYNG] as int) 
			,TRY_CAST([PRO_OPERAZIONE_SGR] as int) 
			,[COD_STATO_MOVIMENTO]
			,[DES_STATO_MOVIMENTO]
			,[COD_CANALE_EVENTO]
			,[DES_CANALE_EVENTO]
			,[COD_UO_INSERIMENTO_EVENTO]
			,[COD_NDG_ORDINANTE]
			,[ORIGINE_MOVIMENTO]
			,[COD_CONFERMA_SIM]
			,[COD_TIPO_OPERAZIONE]
			,TRY_CONVERT (date, stuff(stuff([PROFILO_DAT_COMPILAZIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,[PROFILO_COD_PROFILO_INVESTITORE]
			,[PROFILO_DES_PROFILO_INVESTITORE]
			,[PROFILO_COD_TIPO_PROFILO]
			,[PROFILO_DES_TIPO_PROFILO]
			,[PROFILO_COD_STATO_PROFILO]
			,[PROFILO_DES_STATO_PROFILO]
			,TRY_CONVERT (date, stuff(stuff([CONESPR_DAT_COMPILAZIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,[CONESPR_COD_CONOSC_ESPER]
			,[CONESPR_DES_CONOSC_ESPER]
			,[Row_Id]
		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_MOVIMENTI]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			-- Inserire qui le chiavi nella forma: on [src].[key] = [dst].[key]  
			 -- AND [src].[key] = [dst].[key]
	WHEN not matched THEN INSERT (
			[COD_ABI]
			,[COD_NDG_CONTRATTO]
			,[COD_CONTRATTO]
			,[COD_MANDATO]
			,[DAT_INSERIMENTO_ORDINE]
			,[DAT_MOVIMENTO]
			,[COD_CAUSALE_MOVIMENTO_GP]
			,[DES_CAUSALE_MOVIMENTO_GP]
			,[PRO_MOVIMENTO]
			,[DAT_VALUTA_MOVIMENTO]
			,[IMP_MOVIMENTO]
			,[COD_CONTRATTO_RIFERIMENTO]
			,[COD_ABI_KTO_RIFERIMENTO]
			,[COD_LINEA_PRODOTTO_RIFERIMENTO]
			,[DES_LINEA_PRODOTTO_RIFERIMENTO]
			,[PRO_OPERAZIONE_DYNG]
			,[PRO_OPERAZIONE_SGR]
			,[COD_STATO_MOVIMENTO]
			,[DES_STATO_MOVIMENTO]
			,[COD_CANALE_EVENTO]
			,[DES_CANALE_EVENTO]
			,[COD_UO_INSERIMENTO_EVENTO]
			,[COD_NDG_ORDINANTE]
			,[ORIGINE_MOVIMENTO]
			,[COD_CONFERMA_SIM]
			,[COD_TIPO_OPERAZIONE]
			,[PROFILO_DAT_COMPILAZIONE]
			,[PROFILO_COD_PROFILO_INVESTITORE]
			,[PROFILO_DES_PROFILO_INVESTITORE]
			,[PROFILO_COD_TIPO_PROFILO]
			,[PROFILO_DES_TIPO_PROFILO]
			,[PROFILO_COD_STATO_PROFILO]
			,[PROFILO_DES_STATO_PROFILO]
			,[CONESPR_DAT_COMPILAZIONE]
			,[CONESPR_COD_CONOSC_ESPER]
			,[CONESPR_DES_CONOSC_ESPER]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[COD_ABI]
			,[src].[COD_NDG_CONTRATTO]
			,[src].[COD_CONTRATTO]
			,[src].[COD_MANDATO]
			,[src].[DAT_INSERIMENTO_ORDINE]
			,[src].[DAT_MOVIMENTO]
			,[src].[COD_CAUSALE_MOVIMENTO_GP]
			,[src].[DES_CAUSALE_MOVIMENTO_GP]
			,[src].[PRO_MOVIMENTO]
			,[src].[DAT_VALUTA_MOVIMENTO]
			,[src].[IMP_MOVIMENTO]
			,[src].[COD_CONTRATTO_RIFERIMENTO]
			,[src].[COD_ABI_KTO_RIFERIMENTO]
			,[src].[COD_LINEA_PRODOTTO_RIFERIMENTO]
			,[src].[DES_LINEA_PRODOTTO_RIFERIMENTO]
			,[src].[PRO_OPERAZIONE_DYNG]
			,[src].[PRO_OPERAZIONE_SGR]
			,[src].[COD_STATO_MOVIMENTO]
			,[src].[DES_STATO_MOVIMENTO]
			,[src].[COD_CANALE_EVENTO]
			,[src].[DES_CANALE_EVENTO]
			,[src].[COD_UO_INSERIMENTO_EVENTO]
			,[src].[COD_NDG_ORDINANTE]
			,[src].[ORIGINE_MOVIMENTO]
			,[src].[COD_CONFERMA_SIM]
			,[src].[COD_TIPO_OPERAZIONE]
			,[src].[PROFILO_DAT_COMPILAZIONE]
			,[src].[PROFILO_COD_PROFILO_INVESTITORE]
			,[src].[PROFILO_DES_PROFILO_INVESTITORE]
			,[src].[PROFILO_COD_TIPO_PROFILO]
			,[src].[PROFILO_DES_TIPO_PROFILO]
			,[src].[PROFILO_COD_STATO_PROFILO]
			,[src].[PROFILO_DES_STATO_PROFILO]
			,[src].[CONESPR_DAT_COMPILAZIONE]
			,[src].[CONESPR_COD_CONOSC_ESPER]
			,[src].[CONESPR_DES_CONOSC_ESPER]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[COD_ABI] = [src].[COD_ABI]
			,[COD_NDG_CONTRATTO] = [src].[COD_NDG_CONTRATTO]
			,[COD_CONTRATTO] = [src].[COD_CONTRATTO]
			,[COD_MANDATO] = [src].[COD_MANDATO]
			,[DAT_INSERIMENTO_ORDINE] = [src].[DAT_INSERIMENTO_ORDINE]
			,[DAT_MOVIMENTO] = [src].[DAT_MOVIMENTO]
			,[COD_CAUSALE_MOVIMENTO_GP] = [src].[COD_CAUSALE_MOVIMENTO_GP]
			,[DES_CAUSALE_MOVIMENTO_GP] = [src].[DES_CAUSALE_MOVIMENTO_GP]
			,[PRO_MOVIMENTO] = [src].[PRO_MOVIMENTO]
			,[DAT_VALUTA_MOVIMENTO] = [src].[DAT_VALUTA_MOVIMENTO]
			,[IMP_MOVIMENTO] = [src].[IMP_MOVIMENTO]
			,[COD_CONTRATTO_RIFERIMENTO] = [src].[COD_CONTRATTO_RIFERIMENTO]
			,[COD_ABI_KTO_RIFERIMENTO] = [src].[COD_ABI_KTO_RIFERIMENTO]
			,[COD_LINEA_PRODOTTO_RIFERIMENTO] = [src].[COD_LINEA_PRODOTTO_RIFERIMENTO]
			,[DES_LINEA_PRODOTTO_RIFERIMENTO] = [src].[DES_LINEA_PRODOTTO_RIFERIMENTO]
			,[PRO_OPERAZIONE_DYNG] = [src].[PRO_OPERAZIONE_DYNG]
			,[PRO_OPERAZIONE_SGR] = [src].[PRO_OPERAZIONE_SGR]
			,[COD_STATO_MOVIMENTO] = [src].[COD_STATO_MOVIMENTO]
			,[DES_STATO_MOVIMENTO] = [src].[DES_STATO_MOVIMENTO]
			,[COD_CANALE_EVENTO] = [src].[COD_CANALE_EVENTO]
			,[DES_CANALE_EVENTO] = [src].[DES_CANALE_EVENTO]
			,[COD_UO_INSERIMENTO_EVENTO] = [src].[COD_UO_INSERIMENTO_EVENTO]
			,[COD_NDG_ORDINANTE] = [src].[COD_NDG_ORDINANTE]
			,[ORIGINE_MOVIMENTO] = [src].[ORIGINE_MOVIMENTO]
			,[COD_CONFERMA_SIM] = [src].[COD_CONFERMA_SIM]
			,[COD_TIPO_OPERAZIONE] = [src].[COD_TIPO_OPERAZIONE]
			,[PROFILO_DAT_COMPILAZIONE] = [src].[PROFILO_DAT_COMPILAZIONE]
			,[PROFILO_COD_PROFILO_INVESTITORE] = [src].[PROFILO_COD_PROFILO_INVESTITORE]
			,[PROFILO_DES_PROFILO_INVESTITORE] = [src].[PROFILO_DES_PROFILO_INVESTITORE]
			,[PROFILO_COD_TIPO_PROFILO] = [src].[PROFILO_COD_TIPO_PROFILO]
			,[PROFILO_DES_TIPO_PROFILO] = [src].[PROFILO_DES_TIPO_PROFILO]
			,[PROFILO_COD_STATO_PROFILO] = [src].[PROFILO_COD_STATO_PROFILO]
			,[PROFILO_DES_STATO_PROFILO] = [src].[PROFILO_DES_STATO_PROFILO]
			,[CONESPR_DAT_COMPILAZIONE] = [src].[CONESPR_DAT_COMPILAZIONE]
			,[CONESPR_COD_CONOSC_ESPER] = [src].[CONESPR_COD_CONOSC_ESPER]
			,[CONESPR_DES_CONOSC_ESPER] = [src].[CONESPR_DES_CONOSC_ESPER]
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