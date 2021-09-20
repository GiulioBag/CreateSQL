/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Reclami] alla tabella [L1].[T_Vita_Reclami].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Reclami]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Reclami]
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
		
		UPDATE [L0].[T_Vita_Reclami]
		SET [BitMask_Scarti] = 0 
		; 
 

--		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
--		;
--		UPDATE [sn]
--		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--		FROM [L0].[T_Vita_Reclami] [sn]
--		JOIN ( SELECT 
--			[sn].[COD_ABI_RECLAMATO]
--			,[sn].[COD_FILIALE_RECLAMATA]
--			,[sn].[COD_NDG]
--		FROM [L0].[T_Vita_Reclami] [sn]
--		WHERE [Exec_Id] = @Exec_Id
--			[sn].[COD_ABI_RECLAMATO] <> '' AND [sn].[COD_ABI_RECLAMATO] IS NOT NULL
--			AND [sn].[COD_FILIALE_RECLAMATA] <> '' AND [sn].[COD_FILIALE_RECLAMATA] IS NOT NULL
--			AND [sn].[COD_NDG] <> '' AND [sn].[COD_NDG] IS NOT NULL
--		GROUP BY
--			[sn].[COD_ABI_RECLAMATO]
--			,[sn].[COD_FILIALE_RECLAMATA]
--			,[sn].[COD_NDG]
--		HAVING COUNT(*) > 1
--	) [sn2]
--	on
--		[sn].[COD_ABI_RECLAMATO] = [sn2].[COD_ABI_RECLAMATO]
--		AND [sn].[COD_FILIALE_RECLAMATA] = [sn2].[COD_FILIALE_RECLAMATA]
--		AND [sn].[COD_NDG] = [sn2].[COD_NDG]
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
	FROM [L0].[T_Vita_Reclami] [sn]
	CROSS JOIN[L0_SCARTI].[T_Desc_Scarti][scarti]
	where[Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_KEY' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			([sn].[COD_ABI_RECLAMATO] is null OR [sn].[COD_ABI_RECLAMATO] = '')
			OR ([sn].[COD_FILIALE_RECLAMATA] is null OR [sn].[COD_FILIALE_RECLAMATA] = '')
			OR ([sn].[COD_NDG] is null OR [sn].[COD_NDG] = '')
		)
	; 
 

	--Viene preso solamente una riga tra le N righe duplicate.
	--Viene presa la prima riga del file (Ordinament o per Row_Id ASC)
	SET @Step = '2.3 Scarti: Applicazione criterio di scarto GET_ONE_DUPLICATE_KEY'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Reclami] [sn]
	JOIN (
		SELECT 
			[r].[COD_ABI_RECLAMATO]
			,[r].[COD_FILIALE_RECLAMATA]
			,[r].[COD_NDG]
			,[r].[Row_Id]
			,[r].[Exec_Id]
			,ROW_NUMBER() OVER(
			PARTIOTION BY
					[r].[COD_ABI_RECLAMATO]
					,[r].[COD_FILIALE_RECLAMATA]
					,[r].[COD_NDG]
				ORDER BY
					[r].[Row_Id] ASC
					,[r].[Exec_Id] ASC]
		) as [rn]
		FROM [L0].[T_Vita_Reclami] [r]
		WHERE [Exec_Id] = @Exec_Id
	) [sn2]
		on
			[sn].[COD_ABI_RECLAMATO] = [sn2].[COD_ABI_RECLAMATO]
			AND [sn].[COD_FILIALE_RECLAMATA] = [sn2].[COD_FILIALE_RECLAMATA]
			AND [sn].[COD_NDG] = [sn2].[COD_NDG]
			AND [sn].[Row_Id] = [sn2].[Row_Id]
			AND [sn].[Exec_Id] = [sn2].[Exec_Id]
	[sn].[COD_ABI_RECLAMATO] = [sn2].[COD_ABI_RECLAMATO]
	AND [sn].[COD_FILIALE_RECLAMATA] = [sn2].[COD_FILIALE_RECLAMATA]
	AND [sn].[COD_NDG] = [sn2].[COD_NDG]
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
	FROM [L0].[T_Vita_Reclami] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_CENSIMENTO] is null OR [sn].[DAT_CENSIMENTO] = '')
			--OR ([sn].[DAT_DECORRENZA] is null OR [sn].[DAT_DECORRENZA] = '')
			--OR ([sn].[DAT_ARCHIVIAZIONE] is null OR [sn].[DAT_ARCHIVIAZIONE] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Reclami] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_CENSIMENTO] is not null AND [sn].[DAT_CENSIMENTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_CENSIMENTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_DECORRENZA] is not null AND [sn].[DAT_DECORRENZA]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_DECORRENZA], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_ARCHIVIAZIONE] is not null AND [sn].[DAT_ARCHIVIAZIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_ARCHIVIAZIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Reclami] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[RECLAMO_RICORSO] is null )
			--OR ([sn].[IMP_RICHIESTO] is null )
		)
	; 
 

	--Esclusione numeric non Validi
	SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Reclami] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[RECLAMO_RICORSO] is NOT null AND TRY_CAST([RECLAMO_RICORSO] as int) IS  NULL))
			OR ([sn].[IMP_RICHIESTO] is NOT null AND TRY_CAST(REPLACE([IMP_RICHIESTO], ',', '.') as NUMERIC (22,2)) IS  NULL))
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Vita_Reclami](
		[RECLAMO_RICORSO]
		,[COD_ABI_RECLAMATO]
		,[COD_FILIALE_RECLAMATA]
		,[COD_PROTOCOLLO]
		,[COD_SOGGETTO_GIURIDICO]
		,[COD_TIPO_RECLAMO]
		,[DES_TIPO_RECLAMO]
		,[COD_TIPO_RISCONTRO]
		,[DES_TIPO_RISCONTRO]
		,[COD_SOGGETTO_COINVOLTO_RECLAMO]
		,[DES_SOGGETTO_COINVOLTO_RECLAMO]
		,[DAT_CENSIMENTO]
		,[DAT_DECORRENZA]
		,[COD_ESITO_RECLAMO]
		,[DAT_ARCHIVIAZIONE]
		,[DES_ESITO_RECLAMO]
		,[COD_STATO_RECLAMO]
		,[DES_STATO_RECLAMO]
		,[COD_PRODOTTO]
		,[DES_PRODOTTO]
		,[COD_SUBPRODOTTO]
		,[DES_SUBPRODOTTO]
		,[COD_MACRO_SETTORE]
		,[DES_MACRO_SETTORE]
		,[COD_SUBMOTIVO]
		,[DES_SUBMOTIVO]
		,[COD_MOTIVO]
		,[DES_MOTIVO]
		,[COD_TIPO_CLIENTE]
		,[DES_TIPO_CLIENTE]
		,[COD_MITTENTE]
		,[COD_NDG]
		,[IMP_RICHIESTO]
		,[COD_TIPO_SUBRECLAMO]
		,[DES_SUBTIPO_RECLAMO]
		,[COD_CARATTERISTICA_RECLAMO]
		,[COD_STATO_RICORSO]
		,[COD_ESITO_RICORSO]
		,[DES_ESITO_RICORSO]
		,[DES_STATO_RICORSO]
		,[DES_ARGOMENTO]
		,[DES_NOTE]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[RECLAMO_RICORSO]
		,[COD_ABI_RECLAMATO]
		,[COD_FILIALE_RECLAMATA]
		,[COD_PROTOCOLLO]
		,[COD_SOGGETTO_GIURIDICO]
		,[COD_TIPO_RECLAMO]
		,[DES_TIPO_RECLAMO]
		,[COD_TIPO_RISCONTRO]
		,[DES_TIPO_RISCONTRO]
		,[COD_SOGGETTO_COINVOLTO_RECLAMO]
		,[DES_SOGGETTO_COINVOLTO_RECLAMO]
		,[DAT_CENSIMENTO]
		,[DAT_DECORRENZA]
		,[COD_ESITO_RECLAMO]
		,[DAT_ARCHIVIAZIONE]
		,[DES_ESITO_RECLAMO]
		,[COD_STATO_RECLAMO]
		,[DES_STATO_RECLAMO]
		,[COD_PRODOTTO]
		,[DES_PRODOTTO]
		,[COD_SUBPRODOTTO]
		,[DES_SUBPRODOTTO]
		,[COD_MACRO_SETTORE]
		,[DES_MACRO_SETTORE]
		,[COD_SUBMOTIVO]
		,[DES_SUBMOTIVO]
		,[COD_MOTIVO]
		,[DES_MOTIVO]
		,[COD_TIPO_CLIENTE]
		,[DES_TIPO_CLIENTE]
		,[COD_MITTENTE]
		,[COD_NDG]
		,[IMP_RICHIESTO]
		,[COD_TIPO_SUBRECLAMO]
		,[DES_SUBTIPO_RECLAMO]
		,[COD_CARATTERISTICA_RECLAMO]
		,[COD_STATO_RICORSO]
		,[COD_ESITO_RICORSO]
		,[DES_ESITO_RICORSO]
		,[DES_STATO_RICORSO]
		,[DES_ARGOMENTO]
		,[DES_NOTE]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Reclami]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Reclami] AS dst
	USING 
		( SELECT
			TRY_CAST([RECLAMO_RICORSO] as int) 
			,[COD_ABI_RECLAMATO]
			,[COD_FILIALE_RECLAMATA]
			,[COD_PROTOCOLLO]
			,[COD_SOGGETTO_GIURIDICO]
			,[COD_TIPO_RECLAMO]
			,[DES_TIPO_RECLAMO]
			,[COD_TIPO_RISCONTRO]
			,[DES_TIPO_RISCONTRO]
			,[COD_SOGGETTO_COINVOLTO_RECLAMO]
			,[DES_SOGGETTO_COINVOLTO_RECLAMO]
			,TRY_CONVERT (date, stuff(stuff([DAT_CENSIMENTO], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_DECORRENZA], 6, 0, ' '), 3, 0, ' '), 106)
			,[COD_ESITO_RECLAMO]
			,TRY_CONVERT (date, stuff(stuff([DAT_ARCHIVIAZIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,[DES_ESITO_RECLAMO]
			,[COD_STATO_RECLAMO]
			,[DES_STATO_RECLAMO]
			,[COD_PRODOTTO]
			,[DES_PRODOTTO]
			,[COD_SUBPRODOTTO]
			,[DES_SUBPRODOTTO]
			,[COD_MACRO_SETTORE]
			,[DES_MACRO_SETTORE]
			,[COD_SUBMOTIVO]
			,[DES_SUBMOTIVO]
			,[COD_MOTIVO]
			,[DES_MOTIVO]
			,[COD_TIPO_CLIENTE]
			,[DES_TIPO_CLIENTE]
			,[COD_MITTENTE]
			,[COD_NDG]
			,TRY_CAST(REPLACE([IMP_RICHIESTO], ',', '.') as NUMERIC (22,2)) 
			,[COD_TIPO_SUBRECLAMO]
			,[DES_SUBTIPO_RECLAMO]
			,[COD_CARATTERISTICA_RECLAMO]
			,[COD_STATO_RICORSO]
			,[COD_ESITO_RICORSO]
			,[DES_ESITO_RICORSO]
			,[DES_STATO_RICORSO]
			,[DES_ARGOMENTO]
			,[DES_NOTE]
			,[Row_Id]
		FROM [L0].[T_Vita_Reclami]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			on [src].[COD_ABI_RECLAMATO] = [dst].[COD_ABI_RECLAMATO]
			AND [src].[COD_FILIALE_RECLAMATA] = [dst].[COD_FILIALE_RECLAMATA]
			AND [src].[COD_NDG] = [dst].[COD_NDG]
	WHEN not matched THEN INSERT (
			[RECLAMO_RICORSO]
			,[COD_ABI_RECLAMATO]
			,[COD_FILIALE_RECLAMATA]
			,[COD_PROTOCOLLO]
			,[COD_SOGGETTO_GIURIDICO]
			,[COD_TIPO_RECLAMO]
			,[DES_TIPO_RECLAMO]
			,[COD_TIPO_RISCONTRO]
			,[DES_TIPO_RISCONTRO]
			,[COD_SOGGETTO_COINVOLTO_RECLAMO]
			,[DES_SOGGETTO_COINVOLTO_RECLAMO]
			,[DAT_CENSIMENTO]
			,[DAT_DECORRENZA]
			,[COD_ESITO_RECLAMO]
			,[DAT_ARCHIVIAZIONE]
			,[DES_ESITO_RECLAMO]
			,[COD_STATO_RECLAMO]
			,[DES_STATO_RECLAMO]
			,[COD_PRODOTTO]
			,[DES_PRODOTTO]
			,[COD_SUBPRODOTTO]
			,[DES_SUBPRODOTTO]
			,[COD_MACRO_SETTORE]
			,[DES_MACRO_SETTORE]
			,[COD_SUBMOTIVO]
			,[DES_SUBMOTIVO]
			,[COD_MOTIVO]
			,[DES_MOTIVO]
			,[COD_TIPO_CLIENTE]
			,[DES_TIPO_CLIENTE]
			,[COD_MITTENTE]
			,[COD_NDG]
			,[IMP_RICHIESTO]
			,[COD_TIPO_SUBRECLAMO]
			,[DES_SUBTIPO_RECLAMO]
			,[COD_CARATTERISTICA_RECLAMO]
			,[COD_STATO_RICORSO]
			,[COD_ESITO_RICORSO]
			,[DES_ESITO_RICORSO]
			,[DES_STATO_RICORSO]
			,[DES_ARGOMENTO]
			,[DES_NOTE]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[RECLAMO_RICORSO]
			,[src].[COD_ABI_RECLAMATO]
			,[src].[COD_FILIALE_RECLAMATA]
			,[src].[COD_PROTOCOLLO]
			,[src].[COD_SOGGETTO_GIURIDICO]
			,[src].[COD_TIPO_RECLAMO]
			,[src].[DES_TIPO_RECLAMO]
			,[src].[COD_TIPO_RISCONTRO]
			,[src].[DES_TIPO_RISCONTRO]
			,[src].[COD_SOGGETTO_COINVOLTO_RECLAMO]
			,[src].[DES_SOGGETTO_COINVOLTO_RECLAMO]
			,[src].[DAT_CENSIMENTO]
			,[src].[DAT_DECORRENZA]
			,[src].[COD_ESITO_RECLAMO]
			,[src].[DAT_ARCHIVIAZIONE]
			,[src].[DES_ESITO_RECLAMO]
			,[src].[COD_STATO_RECLAMO]
			,[src].[DES_STATO_RECLAMO]
			,[src].[COD_PRODOTTO]
			,[src].[DES_PRODOTTO]
			,[src].[COD_SUBPRODOTTO]
			,[src].[DES_SUBPRODOTTO]
			,[src].[COD_MACRO_SETTORE]
			,[src].[DES_MACRO_SETTORE]
			,[src].[COD_SUBMOTIVO]
			,[src].[DES_SUBMOTIVO]
			,[src].[COD_MOTIVO]
			,[src].[DES_MOTIVO]
			,[src].[COD_TIPO_CLIENTE]
			,[src].[DES_TIPO_CLIENTE]
			,[src].[COD_MITTENTE]
			,[src].[COD_NDG]
			,[src].[IMP_RICHIESTO]
			,[src].[COD_TIPO_SUBRECLAMO]
			,[src].[DES_SUBTIPO_RECLAMO]
			,[src].[COD_CARATTERISTICA_RECLAMO]
			,[src].[COD_STATO_RICORSO]
			,[src].[COD_ESITO_RICORSO]
			,[src].[DES_ESITO_RICORSO]
			,[src].[DES_STATO_RICORSO]
			,[src].[DES_ARGOMENTO]
			,[src].[DES_NOTE]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[RECLAMO_RICORSO] = [src].[RECLAMO_RICORSO]
			,[COD_PROTOCOLLO] = [src].[COD_PROTOCOLLO]
			,[COD_SOGGETTO_GIURIDICO] = [src].[COD_SOGGETTO_GIURIDICO]
			,[COD_TIPO_RECLAMO] = [src].[COD_TIPO_RECLAMO]
			,[DES_TIPO_RECLAMO] = [src].[DES_TIPO_RECLAMO]
			,[COD_TIPO_RISCONTRO] = [src].[COD_TIPO_RISCONTRO]
			,[DES_TIPO_RISCONTRO] = [src].[DES_TIPO_RISCONTRO]
			,[COD_SOGGETTO_COINVOLTO_RECLAMO] = [src].[COD_SOGGETTO_COINVOLTO_RECLAMO]
			,[DES_SOGGETTO_COINVOLTO_RECLAMO] = [src].[DES_SOGGETTO_COINVOLTO_RECLAMO]
			,[DAT_CENSIMENTO] = [src].[DAT_CENSIMENTO]
			,[DAT_DECORRENZA] = [src].[DAT_DECORRENZA]
			,[COD_ESITO_RECLAMO] = [src].[COD_ESITO_RECLAMO]
			,[DAT_ARCHIVIAZIONE] = [src].[DAT_ARCHIVIAZIONE]
			,[DES_ESITO_RECLAMO] = [src].[DES_ESITO_RECLAMO]
			,[COD_STATO_RECLAMO] = [src].[COD_STATO_RECLAMO]
			,[DES_STATO_RECLAMO] = [src].[DES_STATO_RECLAMO]
			,[COD_PRODOTTO] = [src].[COD_PRODOTTO]
			,[DES_PRODOTTO] = [src].[DES_PRODOTTO]
			,[COD_SUBPRODOTTO] = [src].[COD_SUBPRODOTTO]
			,[DES_SUBPRODOTTO] = [src].[DES_SUBPRODOTTO]
			,[COD_MACRO_SETTORE] = [src].[COD_MACRO_SETTORE]
			,[DES_MACRO_SETTORE] = [src].[DES_MACRO_SETTORE]
			,[COD_SUBMOTIVO] = [src].[COD_SUBMOTIVO]
			,[DES_SUBMOTIVO] = [src].[DES_SUBMOTIVO]
			,[COD_MOTIVO] = [src].[COD_MOTIVO]
			,[DES_MOTIVO] = [src].[DES_MOTIVO]
			,[COD_TIPO_CLIENTE] = [src].[COD_TIPO_CLIENTE]
			,[DES_TIPO_CLIENTE] = [src].[DES_TIPO_CLIENTE]
			,[COD_MITTENTE] = [src].[COD_MITTENTE]
			,[IMP_RICHIESTO] = [src].[IMP_RICHIESTO]
			,[COD_TIPO_SUBRECLAMO] = [src].[COD_TIPO_SUBRECLAMO]
			,[DES_SUBTIPO_RECLAMO] = [src].[DES_SUBTIPO_RECLAMO]
			,[COD_CARATTERISTICA_RECLAMO] = [src].[COD_CARATTERISTICA_RECLAMO]
			,[COD_STATO_RICORSO] = [src].[COD_STATO_RICORSO]
			,[COD_ESITO_RICORSO] = [src].[COD_ESITO_RICORSO]
			,[DES_ESITO_RICORSO] = [src].[DES_ESITO_RICORSO]
			,[DES_STATO_RICORSO] = [src].[DES_STATO_RICORSO]
			,[DES_ARGOMENTO] = [src].[DES_ARGOMENTO]
			,[DES_NOTE] = [src].[DES_NOTE]
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