/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Fondi_Polizze] alla tabella [L1].[T_Vita_Fondi_Polizze].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Fondi_Polizze]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Fondi_Polizze]
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
		
		UPDATE [L0].[T_Vita_Fondi_Polizze]
		SET [BitMask_Scarti] = 0 
		; 
 

--		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
--		;
--		UPDATE [sn]
--		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--		FROM [L0].[T_Vita_Fondi_Polizze] [sn]
--		JOIN ( SELECT 
--			[sn].[COD_ABI]
--			,[sn].[COD_CONTRATTO]
--			,[sn].[COD_FONDO]
--			,[sn].[COD_COMPAGNIA]
--		FROM [L0].[T_Vita_Fondi_Polizze] [sn]
--		WHERE [Exec_Id] = @Exec_Id
--			[sn].[COD_ABI] <> '' AND [sn].[COD_ABI] IS NOT NULL
--			AND [sn].[COD_CONTRATTO] <> '' AND [sn].[COD_CONTRATTO] IS NOT NULL
--			AND [sn].[COD_FONDO] <> '' AND [sn].[COD_FONDO] IS NOT NULL
--			AND [sn].[COD_COMPAGNIA] <> '' AND [sn].[COD_COMPAGNIA] IS NOT NULL
--		GROUP BY
--			[sn].[COD_ABI]
--			,[sn].[COD_CONTRATTO]
--			,[sn].[COD_FONDO]
--			,[sn].[COD_COMPAGNIA]
--		HAVING COUNT(*) > 1
--	) [sn2]
--	on
--		[sn].[COD_ABI] = [sn2].[COD_ABI]
--		AND [sn].[COD_CONTRATTO] = [sn2].[COD_CONTRATTO]
--		AND [sn].[COD_FONDO] = [sn2].[COD_FONDO]
--		AND [sn].[COD_COMPAGNIA] = [sn2].[COD_COMPAGNIA]
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
	FROM [L0].[T_Vita_Fondi_Polizze] [sn]
	CROSS JOIN[L0_SCARTI].[T_Desc_Scarti][scarti]
	where[Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_KEY' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			([sn].[COD_ABI] is null OR [sn].[COD_ABI] = '')
			OR ([sn].[COD_CONTRATTO] is null OR [sn].[COD_CONTRATTO] = '')
			OR ([sn].[COD_FONDO] is null OR [sn].[COD_FONDO] = '')
			OR ([sn].[COD_COMPAGNIA] is null OR [sn].[COD_COMPAGNIA] = '')
		)
	; 
 

	--Viene preso solamente una riga tra le N righe duplicate.
	--Viene presa la prima riga del file (Ordinament o per Row_Id ASC)
	SET @Step = '2.3 Scarti: Applicazione criterio di scarto GET_ONE_DUPLICATE_KEY'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Fondi_Polizze] [sn]
	JOIN (
		SELECT 
			[r].[COD_ABI]
			,[r].[COD_CONTRATTO]
			,[r].[COD_FONDO]
			,[r].[COD_COMPAGNIA]
			,[r].[Row_Id]
			,[r].[Exec_Id]
			,ROW_NUMBER() OVER(
			PARTIOTION BY
					[r].[COD_ABI]
					,[r].[COD_CONTRATTO]
					,[r].[COD_FONDO]
					,[r].[COD_COMPAGNIA]
				ORDER BY
					[r].[Row_Id] ASC
					,[r].[Exec_Id] ASC]
		) as [rn]
		FROM [L0].[T_Vita_Fondi_Polizze] [r]
		WHERE [Exec_Id] = @Exec_Id
	) [sn2]
		on
			[sn].[COD_ABI] = [sn2].[COD_ABI]
			AND [sn].[COD_CONTRATTO] = [sn2].[COD_CONTRATTO]
			AND [sn].[COD_FONDO] = [sn2].[COD_FONDO]
			AND [sn].[COD_COMPAGNIA] = [sn2].[COD_COMPAGNIA]
			AND [sn].[Row_Id] = [sn2].[Row_Id]
			AND [sn].[Exec_Id] = [sn2].[Exec_Id]
	[sn].[COD_ABI] = [sn2].[COD_ABI]
	AND [sn].[COD_CONTRATTO] = [sn2].[COD_CONTRATTO]
	AND [sn].[COD_FONDO] = [sn2].[COD_FONDO]
	AND [sn].[COD_COMPAGNIA] = [sn2].[COD_COMPAGNIA]
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
	FROM [L0].[T_Vita_Fondi_Polizze] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_EMISSIONE] is null OR [sn].[DAT_EMISSIONE] = '')
			--OR ([sn].[DATA_QUOTAZIONE] is null OR [sn].[DATA_QUOTAZIONE] = '')
			--OR ([sn].[DAT_AVVALORAMENTO] is null OR [sn].[DAT_AVVALORAMENTO] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Fondi_Polizze] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_EMISSIONE] is not null AND [sn].[DAT_EMISSIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_EMISSIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DATA_QUOTAZIONE] is not null AND [sn].[DATA_QUOTAZIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DATA_QUOTAZIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_AVVALORAMENTO] is not null AND [sn].[DAT_AVVALORAMENTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_AVVALORAMENTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Fondi_Polizze] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[IMP_QUOTA] is null )
			--OR ([sn].[NUM_QUOTE_OPERAZIONE] is null )
			--OR ([sn].[IMP_CONTROVALORE] is null )
			--OR ([sn].[PERC_CONTROVALORE] is null )
		)
	; 
 

	--Esclusione numeric non Validi
	SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Fondi_Polizze] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[IMP_QUOTA] is NOT null AND TRY_CAST(REPLACE([IMP_QUOTA], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[NUM_QUOTE_OPERAZIONE] is NOT null AND TRY_CAST([NUM_QUOTE_OPERAZIONE] as int) IS  NULL))
			OR ([sn].[IMP_CONTROVALORE] is NOT null AND TRY_CAST(REPLACE([IMP_CONTROVALORE], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[PERC_CONTROVALORE] is NOT null AND TRY_CAST(REPLACE([PERC_CONTROVALORE], ',', '.') as NUMERIC (22,2)) IS  NULL))
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Vita_Fondi_Polizze](
		[COD_ABI]
		,[COD_CONTRATTO]
		,[COD_FONDO]
		,[COD_COMPAGNIA]
		,[DAT_EMISSIONE]
		,[DATA_QUOTAZIONE]
		,[IMP_QUOTA]
		,[NUM_QUOTE_OPERAZIONE]
		,[IMP_CONTROVALORE]
		,[PERC_CONTROVALORE]
		,[DAT_AVVALORAMENTO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[COD_ABI]
		,[COD_CONTRATTO]
		,[COD_FONDO]
		,[COD_COMPAGNIA]
		,[DAT_EMISSIONE]
		,[DATA_QUOTAZIONE]
		,[IMP_QUOTA]
		,[NUM_QUOTE_OPERAZIONE]
		,[IMP_CONTROVALORE]
		,[PERC_CONTROVALORE]
		,[DAT_AVVALORAMENTO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Fondi_Polizze]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Fondi_Polizze] AS dst
	USING 
		( SELECT
			[COD_ABI]
			,[COD_CONTRATTO]
			,[COD_FONDO]
			,[COD_COMPAGNIA]
			,TRY_CONVERT (date, stuff(stuff([DAT_EMISSIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DATA_QUOTAZIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CAST(REPLACE([IMP_QUOTA], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST([NUM_QUOTE_OPERAZIONE] as int) 
			,TRY_CAST(REPLACE([IMP_CONTROVALORE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([PERC_CONTROVALORE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CONVERT (date, stuff(stuff([DAT_AVVALORAMENTO], 6, 0, ' '), 3, 0, ' '), 106)
			,[Row_Id]
		FROM [L0].[T_Vita_Fondi_Polizze]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			on [src].[COD_ABI] = [dst].[COD_ABI]
			AND [src].[COD_CONTRATTO] = [dst].[COD_CONTRATTO]
			AND [src].[COD_FONDO] = [dst].[COD_FONDO]
			AND [src].[COD_COMPAGNIA] = [dst].[COD_COMPAGNIA]
	WHEN not matched THEN INSERT (
			[COD_ABI]
			,[COD_CONTRATTO]
			,[COD_FONDO]
			,[COD_COMPAGNIA]
			,[DAT_EMISSIONE]
			,[DATA_QUOTAZIONE]
			,[IMP_QUOTA]
			,[NUM_QUOTE_OPERAZIONE]
			,[IMP_CONTROVALORE]
			,[PERC_CONTROVALORE]
			,[DAT_AVVALORAMENTO]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[COD_ABI]
			,[src].[COD_CONTRATTO]
			,[src].[COD_FONDO]
			,[src].[COD_COMPAGNIA]
			,[src].[DAT_EMISSIONE]
			,[src].[DATA_QUOTAZIONE]
			,[src].[IMP_QUOTA]
			,[src].[NUM_QUOTE_OPERAZIONE]
			,[src].[IMP_CONTROVALORE]
			,[src].[PERC_CONTROVALORE]
			,[src].[DAT_AVVALORAMENTO]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[DAT_EMISSIONE] = [src].[DAT_EMISSIONE]
			,[DATA_QUOTAZIONE] = [src].[DATA_QUOTAZIONE]
			,[IMP_QUOTA] = [src].[IMP_QUOTA]
			,[NUM_QUOTE_OPERAZIONE] = [src].[NUM_QUOTE_OPERAZIONE]
			,[IMP_CONTROVALORE] = [src].[IMP_CONTROVALORE]
			,[PERC_CONTROVALORE] = [src].[PERC_CONTROVALORE]
			,[DAT_AVVALORAMENTO] = [src].[DAT_AVVALORAMENTO]
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