/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Trasformazioni] alla tabella [L1].[T_Vita_Trasformazioni].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Trasformazioni]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Trasformazioni]
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
		
		UPDATE [L0].[T_Vita_Trasformazioni]
		SET [BitMask_Scarti] = 0 
		; 
 

--		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
--		;
--		UPDATE [sn]
--		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--		FROM [L0].[T_Vita_Trasformazioni] [sn]
--		JOIN ( SELECT 
--			[sn].[COD_ABI]
--			,[sn].[COD_NDG_CONTRATTUALE]
--		FROM [L0].[T_Vita_Trasformazioni] [sn]
--		WHERE [Exec_Id] = @Exec_Id
--			[sn].[COD_ABI] <> '' AND [sn].[COD_ABI] IS NOT NULL
--			AND [sn].[COD_NDG_CONTRATTUALE] <> '' AND [sn].[COD_NDG_CONTRATTUALE] IS NOT NULL
--		GROUP BY
--			[sn].[COD_ABI]
--			,[sn].[COD_NDG_CONTRATTUALE]
--		HAVING COUNT(*) > 1
--	) [sn2]
--	on
--		[sn].[COD_ABI] = [sn2].[COD_ABI]
--		AND [sn].[COD_NDG_CONTRATTUALE] = [sn2].[COD_NDG_CONTRATTUALE]
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
	FROM [L0].[T_Vita_Trasformazioni] [sn]
	CROSS JOIN[L0_SCARTI].[T_Desc_Scarti][scarti]
	where[Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_KEY' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			([sn].[COD_ABI] is null OR [sn].[COD_ABI] = '')
			OR ([sn].[COD_NDG_CONTRATTUALE] is null OR [sn].[COD_NDG_CONTRATTUALE] = '')
		)
	; 
 

	--Viene preso solamente una riga tra le N righe duplicate.
	--Viene presa la prima riga del file (Ordinament o per Row_Id ASC)
	SET @Step = '2.3 Scarti: Applicazione criterio di scarto GET_ONE_DUPLICATE_KEY'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Trasformazioni] [sn]
	JOIN (
		SELECT 
			[r].[COD_ABI]
			,[r].[COD_NDG_CONTRATTUALE]
			,[r].[Row_Id]
			,[r].[Exec_Id]
			,ROW_NUMBER() OVER(
			PARTIOTION BY
					[r].[COD_ABI]
					,[r].[COD_NDG_CONTRATTUALE]
				ORDER BY
					[r].[Row_Id] ASC
					,[r].[Exec_Id] ASC]
		) as [rn]
		FROM [L0].[T_Vita_Trasformazioni] [r]
		WHERE [Exec_Id] = @Exec_Id
	) [sn2]
		on
			[sn].[COD_ABI] = [sn2].[COD_ABI]
			AND [sn].[COD_NDG_CONTRATTUALE] = [sn2].[COD_NDG_CONTRATTUALE]
			AND [sn].[Row_Id] = [sn2].[Row_Id]
			AND [sn].[Exec_Id] = [sn2].[Exec_Id]
	[sn].[COD_ABI] = [sn2].[COD_ABI]
	AND [sn].[COD_NDG_CONTRATTUALE] = [sn2].[COD_NDG_CONTRATTUALE]
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
	FROM [L0].[T_Vita_Trasformazioni] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_RISCATTO] is null OR [sn].[DAT_RISCATTO] = '')
			--OR ([sn].[DAT_EMISSIONE] is null OR [sn].[DAT_EMISSIONE] = '')
			--OR ([sn].[DATA_TRASFORMAZIONE] is null OR [sn].[DATA_TRASFORMAZIONE] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Trasformazioni] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_RISCATTO] is not null AND [sn].[DAT_RISCATTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_RISCATTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_EMISSIONE] is not null AND [sn].[DAT_EMISSIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_EMISSIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DATA_TRASFORMAZIONE] is not null AND [sn].[DATA_TRASFORMAZIONE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DATA_TRASFORMAZIONE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Trasformazioni] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[IMP_EMISSIONE] is null )
			--OR ([sn].[IMP_RISCATTO] is null )
		)
	; 
 

	--Esclusione numeric non Validi
	SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Trasformazioni] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[IMP_EMISSIONE] is NOT null AND TRY_CAST(REPLACE([IMP_EMISSIONE], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[IMP_RISCATTO] is NOT null AND TRY_CAST(REPLACE([IMP_RISCATTO], ',', '.') as NUMERIC (22,2)) IS  NULL))
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Vita_Trasformazioni](
		[COD_ABI]
		,[COD_NDG_CONTRATTUALE]
		,[COD_CONTRATTO_RISC]
		,[COD_POLIZZA_RISC]
		,[COD_COMPAGNIA_RISC]
		,[COD_TARIFFA_ENTE_RISC]
		,[COD_PRODOTTO_INTERNO_RISC]
		,[DES_PRODOTTO_RISC]
		,[COD_TIPO_LIQUIDIZIONE_RISC]
		,[DES_TIPO_LIQUIDAZIONE]
		,[DAT_RISCATTO]
		,[COD_CONTRATTO_EMIS]
		,[COD_POLIZZA_EMIS]
		,[COD_COMPAGNIA_EMIS]
		,[COD_TARIFFA_ENTE_EMIS]
		,[COD_PRODOTTO_INTERNO_EMIS]
		,[DES_PRODOTTO]
		,[DAT_EMISSIONE]
		,[trasf]
		,[IMP_EMISSIONE]
		,[IMP_RISCATTO]
		,[CONTROVALORE]
		,[DATA_TRASFORMAZIONE]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[COD_ABI]
		,[COD_NDG_CONTRATTUALE]
		,[COD_CONTRATTO_RISC]
		,[COD_POLIZZA_RISC]
		,[COD_COMPAGNIA_RISC]
		,[COD_TARIFFA_ENTE_RISC]
		,[COD_PRODOTTO_INTERNO_RISC]
		,[DES_PRODOTTO_RISC]
		,[COD_TIPO_LIQUIDIZIONE_RISC]
		,[DES_TIPO_LIQUIDAZIONE]
		,[DAT_RISCATTO]
		,[COD_CONTRATTO_EMIS]
		,[COD_POLIZZA_EMIS]
		,[COD_COMPAGNIA_EMIS]
		,[COD_TARIFFA_ENTE_EMIS]
		,[COD_PRODOTTO_INTERNO_EMIS]
		,[DES_PRODOTTO]
		,[DAT_EMISSIONE]
		,[trasf]
		,[IMP_EMISSIONE]
		,[IMP_RISCATTO]
		,[CONTROVALORE]
		,[DATA_TRASFORMAZIONE]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Trasformazioni]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Trasformazioni] AS dst
	USING 
		( SELECT
			[COD_ABI]
			,[COD_NDG_CONTRATTUALE]
			,[COD_CONTRATTO_RISC]
			,[COD_POLIZZA_RISC]
			,[COD_COMPAGNIA_RISC]
			,[COD_TARIFFA_ENTE_RISC]
			,[COD_PRODOTTO_INTERNO_RISC]
			,[DES_PRODOTTO_RISC]
			,[COD_TIPO_LIQUIDIZIONE_RISC]
			,[DES_TIPO_LIQUIDAZIONE]
			,TRY_CONVERT (date, stuff(stuff([DAT_RISCATTO], 6, 0, ' '), 3, 0, ' '), 106)
			,[COD_CONTRATTO_EMIS]
			,[COD_POLIZZA_EMIS]
			,[COD_COMPAGNIA_EMIS]
			,[COD_TARIFFA_ENTE_EMIS]
			,[COD_PRODOTTO_INTERNO_EMIS]
			,[DES_PRODOTTO]
			,TRY_CONVERT (date, stuff(stuff([DAT_EMISSIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,[trasf]
			,TRY_CAST(REPLACE([IMP_EMISSIONE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([IMP_RISCATTO], ',', '.') as NUMERIC (22,2)) 
			,[CONTROVALORE]
			,TRY_CONVERT (date, stuff(stuff([DATA_TRASFORMAZIONE], 6, 0, ' '), 3, 0, ' '), 106)
			,[Row_Id]
		FROM [L0].[T_Vita_Trasformazioni]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			on [src].[COD_ABI] = [dst].[COD_ABI]
			AND [src].[COD_NDG_CONTRATTUALE] = [dst].[COD_NDG_CONTRATTUALE]
	WHEN not matched THEN INSERT (
			[COD_ABI]
			,[COD_NDG_CONTRATTUALE]
			,[COD_CONTRATTO_RISC]
			,[COD_POLIZZA_RISC]
			,[COD_COMPAGNIA_RISC]
			,[COD_TARIFFA_ENTE_RISC]
			,[COD_PRODOTTO_INTERNO_RISC]
			,[DES_PRODOTTO_RISC]
			,[COD_TIPO_LIQUIDIZIONE_RISC]
			,[DES_TIPO_LIQUIDAZIONE]
			,[DAT_RISCATTO]
			,[COD_CONTRATTO_EMIS]
			,[COD_POLIZZA_EMIS]
			,[COD_COMPAGNIA_EMIS]
			,[COD_TARIFFA_ENTE_EMIS]
			,[COD_PRODOTTO_INTERNO_EMIS]
			,[DES_PRODOTTO]
			,[DAT_EMISSIONE]
			,[trasf]
			,[IMP_EMISSIONE]
			,[IMP_RISCATTO]
			,[CONTROVALORE]
			,[DATA_TRASFORMAZIONE]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[COD_ABI]
			,[src].[COD_NDG_CONTRATTUALE]
			,[src].[COD_CONTRATTO_RISC]
			,[src].[COD_POLIZZA_RISC]
			,[src].[COD_COMPAGNIA_RISC]
			,[src].[COD_TARIFFA_ENTE_RISC]
			,[src].[COD_PRODOTTO_INTERNO_RISC]
			,[src].[DES_PRODOTTO_RISC]
			,[src].[COD_TIPO_LIQUIDIZIONE_RISC]
			,[src].[DES_TIPO_LIQUIDAZIONE]
			,[src].[DAT_RISCATTO]
			,[src].[COD_CONTRATTO_EMIS]
			,[src].[COD_POLIZZA_EMIS]
			,[src].[COD_COMPAGNIA_EMIS]
			,[src].[COD_TARIFFA_ENTE_EMIS]
			,[src].[COD_PRODOTTO_INTERNO_EMIS]
			,[src].[DES_PRODOTTO]
			,[src].[DAT_EMISSIONE]
			,[src].[trasf]
			,[src].[IMP_EMISSIONE]
			,[src].[IMP_RISCATTO]
			,[src].[CONTROVALORE]
			,[src].[DATA_TRASFORMAZIONE]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[COD_CONTRATTO_RISC] = [src].[COD_CONTRATTO_RISC]
			,[COD_POLIZZA_RISC] = [src].[COD_POLIZZA_RISC]
			,[COD_COMPAGNIA_RISC] = [src].[COD_COMPAGNIA_RISC]
			,[COD_TARIFFA_ENTE_RISC] = [src].[COD_TARIFFA_ENTE_RISC]
			,[COD_PRODOTTO_INTERNO_RISC] = [src].[COD_PRODOTTO_INTERNO_RISC]
			,[DES_PRODOTTO_RISC] = [src].[DES_PRODOTTO_RISC]
			,[COD_TIPO_LIQUIDIZIONE_RISC] = [src].[COD_TIPO_LIQUIDIZIONE_RISC]
			,[DES_TIPO_LIQUIDAZIONE] = [src].[DES_TIPO_LIQUIDAZIONE]
			,[DAT_RISCATTO] = [src].[DAT_RISCATTO]
			,[COD_CONTRATTO_EMIS] = [src].[COD_CONTRATTO_EMIS]
			,[COD_POLIZZA_EMIS] = [src].[COD_POLIZZA_EMIS]
			,[COD_COMPAGNIA_EMIS] = [src].[COD_COMPAGNIA_EMIS]
			,[COD_TARIFFA_ENTE_EMIS] = [src].[COD_TARIFFA_ENTE_EMIS]
			,[COD_PRODOTTO_INTERNO_EMIS] = [src].[COD_PRODOTTO_INTERNO_EMIS]
			,[DES_PRODOTTO] = [src].[DES_PRODOTTO]
			,[DAT_EMISSIONE] = [src].[DAT_EMISSIONE]
			,[trasf] = [src].[trasf]
			,[IMP_EMISSIONE] = [src].[IMP_EMISSIONE]
			,[IMP_RISCATTO] = [src].[IMP_RISCATTO]
			,[CONTROVALORE] = [src].[CONTROVALORE]
			,[DATA_TRASFORMAZIONE] = [src].[DATA_TRASFORMAZIONE]
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