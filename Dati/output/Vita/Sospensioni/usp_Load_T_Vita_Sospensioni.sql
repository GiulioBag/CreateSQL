/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Sospensioni] alla tabella [L1].[T_Vita_Sospensioni].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Sospensioni]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Sospensioni]
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
		
		UPDATE [L0].[T_Vita_Sospensioni]
		SET [BitMask_Scarti] = 0 
		; 
 

--		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
--		;
--		UPDATE [sn]
--		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--		FROM [L0].[T_Vita_Sospensioni] [sn]
--		JOIN ( SELECT 
--			[sn].[COD_ABI]
--			,[sn].[COD_CONTRATTO]
--			,[sn].[COD_OPERAZIONE]
--			,[sn].[DAT_OPERAZIONE]
--		FROM [L0].[T_Vita_Sospensioni] [sn]
--		WHERE [Exec_Id] = @Exec_Id
--			[sn].[COD_ABI] <> '' AND [sn].[COD_ABI] IS NOT NULL
--			AND [sn].[COD_CONTRATTO] <> '' AND [sn].[COD_CONTRATTO] IS NOT NULL
--			AND [sn].[COD_OPERAZIONE] <> '' AND [sn].[COD_OPERAZIONE] IS NOT NULL
--			AND [sn].[DAT_OPERAZIONE] <> '' AND [sn].[DAT_OPERAZIONE] IS NOT NULL
--		GROUP BY
--			[sn].[COD_ABI]
--			,[sn].[COD_CONTRATTO]
--			,[sn].[COD_OPERAZIONE]
--			,[sn].[DAT_OPERAZIONE]
--		HAVING COUNT(*) > 1
--	) [sn2]
--	on
--		[sn].[COD_ABI] = [sn2].[COD_ABI]
--		AND [sn].[COD_CONTRATTO] = [sn2].[COD_CONTRATTO]
--		AND [sn].[COD_OPERAZIONE] = [sn2].[COD_OPERAZIONE]
--		AND [sn].[DAT_OPERAZIONE] = [sn2].[DAT_OPERAZIONE]
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
	FROM [L0].[T_Vita_Sospensioni] [sn]
	CROSS JOIN[L0_SCARTI].[T_Desc_Scarti][scarti]
	where[Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_KEY' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			([sn].[COD_ABI] is null OR [sn].[COD_ABI] = '')
			OR ([sn].[COD_CONTRATTO] is null OR [sn].[COD_CONTRATTO] = '')
			OR ([sn].[COD_OPERAZIONE] is null OR [sn].[COD_OPERAZIONE] = '')
			OR ([sn].[DAT_OPERAZIONE] is null OR [sn].[DAT_OPERAZIONE] = '')
		)
	; 
 

	--Viene preso solamente una riga tra le N righe duplicate.
	--Viene presa la prima riga del file (Ordinament o per Row_Id ASC)
	SET @Step = '2.3 Scarti: Applicazione criterio di scarto GET_ONE_DUPLICATE_KEY'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Sospensioni] [sn]
	JOIN (
		SELECT 
			[r].[COD_ABI]
			,[r].[COD_CONTRATTO]
			,[r].[COD_OPERAZIONE]
			,[r].[DAT_OPERAZIONE]
			,[r].[Row_Id]
			,[r].[Exec_Id]
			,ROW_NUMBER() OVER(
			PARTIOTION BY
					[r].[COD_ABI]
					,[r].[COD_CONTRATTO]
					,[r].[COD_OPERAZIONE]
					,[r].[DAT_OPERAZIONE]
				ORDER BY
					[r].[Row_Id] ASC
					,[r].[Exec_Id] ASC]
		) as [rn]
		FROM [L0].[T_Vita_Sospensioni] [r]
		WHERE [Exec_Id] = @Exec_Id
	) [sn2]
		on
			[sn].[COD_ABI] = [sn2].[COD_ABI]
			AND [sn].[COD_CONTRATTO] = [sn2].[COD_CONTRATTO]
			AND [sn].[COD_OPERAZIONE] = [sn2].[COD_OPERAZIONE]
			AND [sn].[DAT_OPERAZIONE] = [sn2].[DAT_OPERAZIONE]
			AND [sn].[Row_Id] = [sn2].[Row_Id]
			AND [sn].[Exec_Id] = [sn2].[Exec_Id]
	[sn].[COD_ABI] = [sn2].[COD_ABI]
	AND [sn].[COD_CONTRATTO] = [sn2].[COD_CONTRATTO]
	AND [sn].[COD_OPERAZIONE] = [sn2].[COD_OPERAZIONE]
	AND [sn].[DAT_OPERAZIONE] = [sn2].[DAT_OPERAZIONE]
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
	FROM [L0].[T_Vita_Sospensioni] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_INIZIO_SOSPENSIONE] is null OR [sn].[DAT_INIZIO_SOSPENSIONE] = '')
			--OR ([sn].[DAT_FINE_SOSPENSIONE] is null OR [sn].[DAT_FINE_SOSPENSIONE] = '')
			--OR ([sn].[DAT_OPERAZIONE] is null OR [sn].[DAT_OPERAZIONE] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Sospensioni] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_INIZIO_SOSPENSIONE] is not null AND [sn].[DAT_INIZIO_SOSPENSIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_INIZIO_SOSPENSIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_FINE_SOSPENSIONE] is not null AND [sn].[DAT_FINE_SOSPENSIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_FINE_SOSPENSIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_OPERAZIONE] is not null AND [sn].[DAT_OPERAZIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_OPERAZIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Vita_Sospensioni](
		[COD_ABI]
		,[COD_CONTRATTO]
		,[COD_RICHIEDENTE]
		,[DES_RICHIEDENTE]
		,[DAT_INIZIO_SOSPENSIONE]
		,[DAT_FINE_SOSPENSIONE]
		,[COD_OPERAZIONE]
		,[DES_OPERAZIONE]
		,[DAT_OPERAZIONE]
		,[COD_ESITO_ADEGUATEZZA]
		,[DES_ESITO_ADEGUATEZZA]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[COD_ABI]
		,[COD_CONTRATTO]
		,[COD_RICHIEDENTE]
		,[DES_RICHIEDENTE]
		,[DAT_INIZIO_SOSPENSIONE]
		,[DAT_FINE_SOSPENSIONE]
		,[COD_OPERAZIONE]
		,[DES_OPERAZIONE]
		,[DAT_OPERAZIONE]
		,[COD_ESITO_ADEGUATEZZA]
		,[DES_ESITO_ADEGUATEZZA]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Sospensioni]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Sospensioni] AS dst
	USING 
		( SELECT
			[COD_ABI]
			,[COD_CONTRATTO]
			,[COD_RICHIEDENTE]
			,[DES_RICHIEDENTE]
			,TRY_CONVERT (date, stuff(stuff([DAT_INIZIO_SOSPENSIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_FINE_SOSPENSIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,[COD_OPERAZIONE]
			,[DES_OPERAZIONE]
			,TRY_CONVERT (date, stuff(stuff([DAT_OPERAZIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,[COD_ESITO_ADEGUATEZZA]
			,[DES_ESITO_ADEGUATEZZA]
			,[Row_Id]
		FROM [L0].[T_Vita_Sospensioni]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			on [src].[COD_ABI] = [dst].[COD_ABI]
			AND [src].[COD_CONTRATTO] = [dst].[COD_CONTRATTO]
			AND [src].[COD_OPERAZIONE] = [dst].[COD_OPERAZIONE]
			AND [src].[DAT_OPERAZIONE] = [dst].[DAT_OPERAZIONE]
	WHEN not matched THEN INSERT (
			[COD_ABI]
			,[COD_CONTRATTO]
			,[COD_RICHIEDENTE]
			,[DES_RICHIEDENTE]
			,[DAT_INIZIO_SOSPENSIONE]
			,[DAT_FINE_SOSPENSIONE]
			,[COD_OPERAZIONE]
			,[DES_OPERAZIONE]
			,[DAT_OPERAZIONE]
			,[COD_ESITO_ADEGUATEZZA]
			,[DES_ESITO_ADEGUATEZZA]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[COD_ABI]
			,[src].[COD_CONTRATTO]
			,[src].[COD_RICHIEDENTE]
			,[src].[DES_RICHIEDENTE]
			,[src].[DAT_INIZIO_SOSPENSIONE]
			,[src].[DAT_FINE_SOSPENSIONE]
			,[src].[COD_OPERAZIONE]
			,[src].[DES_OPERAZIONE]
			,[src].[DAT_OPERAZIONE]
			,[src].[COD_ESITO_ADEGUATEZZA]
			,[src].[DES_ESITO_ADEGUATEZZA]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[COD_RICHIEDENTE] = [src].[COD_RICHIEDENTE]
			,[DES_RICHIEDENTE] = [src].[DES_RICHIEDENTE]
			,[DAT_INIZIO_SOSPENSIONE] = [src].[DAT_INIZIO_SOSPENSIONE]
			,[DAT_FINE_SOSPENSIONE] = [src].[DAT_FINE_SOSPENSIONE]
			,[DES_OPERAZIONE] = [src].[DES_OPERAZIONE]
			,[COD_ESITO_ADEGUATEZZA] = [src].[COD_ESITO_ADEGUATEZZA]
			,[DES_ESITO_ADEGUATEZZA] = [src].[DES_ESITO_ADEGUATEZZA]
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