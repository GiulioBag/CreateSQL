/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Struttura_Organizzativa] alla tabella [L1].[T_Vita_Struttura_Organizzativa].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Struttura_Organizzativa]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Struttura_Organizzativa]
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
		
		UPDATE [L0].[T_Vita_Struttura_Organizzativa]
		SET [BitMask_Scarti] = 0 
		; 
 

----		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
----		;
----		UPDATE [sn]
----		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
----		FROM [L0].[T_Vita_Struttura_Organizzativa] [sn]
----		JOIN ( SELECT 
----			Inserire qui le eventuali chiavi nella forma [sn].[key]
----		FROM [L0].[T_Vita_Struttura_Organizzativa] [sn]
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
--	FROM [L0].[T_Vita_Struttura_Organizzativa] [sn]
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
--	FROM [L0].[T_Vita_Struttura_Organizzativa] [sn]
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
--		FROM [L0].[T_Vita_Struttura_Organizzativa] [r]
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
	SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Struttura_Organizzativa] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[ANNOMESE] is null )
			--OR ([sn].[COD_DW_FILIALE] is null )
			--OR ([sn].[GEO_COORDINATA_X] is null )
			--OR ([sn].[GEO_COORDINATA_Y] is null )
		)
	; 
 

	--Esclusione numeric non Validi
	SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Struttura_Organizzativa] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[ANNOMESE] is NOT null AND TRY_CAST([ANNOMESE] as int) IS  NULL))
			OR ([sn].[COD_DW_FILIALE] is NOT null AND TRY_CAST([COD_DW_FILIALE] as int) IS  NULL))
			OR ([sn].[GEO_COORDINATA_X] is NOT null AND TRY_CAST(REPLACE([GEO_COORDINATA_X], ',', '.') as NUMERIC (17,6)) IS  NULL))
			OR ([sn].[GEO_COORDINATA_Y] is NOT null AND TRY_CAST(REPLACE([GEO_COORDINATA_Y], ',', '.') as NUMERIC (17,6)) IS  NULL))
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Vita_Struttura_Organizzativa](
		[ANNOMESE]
		,[COD_DIREZIONE_REGIONALE]
		,[DES_DIREZIONE_REGIONALE]
		,[NOM_INDIRIZZO_DIR_REGIONALE]
		,[NOM_COMUNE_DIR_REGIONALE]
		,[COD_CAP_DIR_REGIONALE]
		,[COD_PROVINCIA_DIR_REGIONALE]
		,[COD_DIREZIONE_COMMERCIALE]
		,[DES_DIREZIONE_COMMERCIALE]
		,[NOM_INDIRIZZO_DIR_COMMERCIALE]
		,[NOM_COMUNE_DIR_COMMERCIALE]
		,[COD_CAP_DIR_COMMERCIALE]
		,[COD_PROVINCIA_DIR_COMMERCIALE]
		,[COD_AREA]
		,[DES_AREA]
		,[NOM_INDIRIZZO_AREA]
		,[NOM_COMUNE_AREA]
		,[COD_CAP_AREA]
		,[COD_PROVINCIA_AREA]
		,[COD_ABI]
		,[COD_FILIALE_UFFICIO]
		,[DES_FILIALE_UFFICIO]
		,[COD_DW_FILIALE]
		,[NOM_INDIRIZZO_FILIALE_UFFICIO]
		,[NOM_LOCALITA_FILIALE_UFFICIO]
		,[NOM_COMUNE_FILIALE_UFFICIO]
		,[COD_CAP_FILIALE_UFFICIO]
		,[NOM_PROVINCIA_FILIALE_UFFICIO]
		,[GEO_COORDINATA_X]
		,[GEO_COORDINATA_Y]
		,[COD_CLASSE_FILIALE_UFFICIO]
		,[DES_CLASSE_FILIALE_UFFICIO]
		,[TERRITORIO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[ANNOMESE]
		,[COD_DIREZIONE_REGIONALE]
		,[DES_DIREZIONE_REGIONALE]
		,[NOM_INDIRIZZO_DIR_REGIONALE]
		,[NOM_COMUNE_DIR_REGIONALE]
		,[COD_CAP_DIR_REGIONALE]
		,[COD_PROVINCIA_DIR_REGIONALE]
		,[COD_DIREZIONE_COMMERCIALE]
		,[DES_DIREZIONE_COMMERCIALE]
		,[NOM_INDIRIZZO_DIR_COMMERCIALE]
		,[NOM_COMUNE_DIR_COMMERCIALE]
		,[COD_CAP_DIR_COMMERCIALE]
		,[COD_PROVINCIA_DIR_COMMERCIALE]
		,[COD_AREA]
		,[DES_AREA]
		,[NOM_INDIRIZZO_AREA]
		,[NOM_COMUNE_AREA]
		,[COD_CAP_AREA]
		,[COD_PROVINCIA_AREA]
		,[COD_ABI]
		,[COD_FILIALE_UFFICIO]
		,[DES_FILIALE_UFFICIO]
		,[COD_DW_FILIALE]
		,[NOM_INDIRIZZO_FILIALE_UFFICIO]
		,[NOM_LOCALITA_FILIALE_UFFICIO]
		,[NOM_COMUNE_FILIALE_UFFICIO]
		,[COD_CAP_FILIALE_UFFICIO]
		,[NOM_PROVINCIA_FILIALE_UFFICIO]
		,[GEO_COORDINATA_X]
		,[GEO_COORDINATA_Y]
		,[COD_CLASSE_FILIALE_UFFICIO]
		,[DES_CLASSE_FILIALE_UFFICIO]
		,[TERRITORIO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Struttura_Organizzativa]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Struttura_Organizzativa] AS dst
	USING 
		( SELECT
			TRY_CAST([ANNOMESE] as int) 
			,[COD_DIREZIONE_REGIONALE]
			,[DES_DIREZIONE_REGIONALE]
			,[NOM_INDIRIZZO_DIR_REGIONALE]
			,[NOM_COMUNE_DIR_REGIONALE]
			,[COD_CAP_DIR_REGIONALE]
			,[COD_PROVINCIA_DIR_REGIONALE]
			,[COD_DIREZIONE_COMMERCIALE]
			,[DES_DIREZIONE_COMMERCIALE]
			,[NOM_INDIRIZZO_DIR_COMMERCIALE]
			,[NOM_COMUNE_DIR_COMMERCIALE]
			,[COD_CAP_DIR_COMMERCIALE]
			,[COD_PROVINCIA_DIR_COMMERCIALE]
			,[COD_AREA]
			,[DES_AREA]
			,[NOM_INDIRIZZO_AREA]
			,[NOM_COMUNE_AREA]
			,[COD_CAP_AREA]
			,[COD_PROVINCIA_AREA]
			,[COD_ABI]
			,[COD_FILIALE_UFFICIO]
			,[DES_FILIALE_UFFICIO]
			,TRY_CAST([COD_DW_FILIALE] as int) 
			,[NOM_INDIRIZZO_FILIALE_UFFICIO]
			,[NOM_LOCALITA_FILIALE_UFFICIO]
			,[NOM_COMUNE_FILIALE_UFFICIO]
			,[COD_CAP_FILIALE_UFFICIO]
			,[NOM_PROVINCIA_FILIALE_UFFICIO]
			,TRY_CAST(REPLACE([GEO_COORDINATA_X], ',', '.') as NUMERIC (17,6)) 
			,TRY_CAST(REPLACE([GEO_COORDINATA_Y], ',', '.') as NUMERIC (17,6)) 
			,[COD_CLASSE_FILIALE_UFFICIO]
			,[DES_CLASSE_FILIALE_UFFICIO]
			,[TERRITORIO]
			,[Row_Id]
		FROM [L0].[T_Vita_Struttura_Organizzativa]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			-- Inserire qui le chiavi nella forma: on [src].[key] = [dst].[key]  
			 -- AND [src].[key] = [dst].[key]
	WHEN not matched THEN INSERT (
			[ANNOMESE]
			,[COD_DIREZIONE_REGIONALE]
			,[DES_DIREZIONE_REGIONALE]
			,[NOM_INDIRIZZO_DIR_REGIONALE]
			,[NOM_COMUNE_DIR_REGIONALE]
			,[COD_CAP_DIR_REGIONALE]
			,[COD_PROVINCIA_DIR_REGIONALE]
			,[COD_DIREZIONE_COMMERCIALE]
			,[DES_DIREZIONE_COMMERCIALE]
			,[NOM_INDIRIZZO_DIR_COMMERCIALE]
			,[NOM_COMUNE_DIR_COMMERCIALE]
			,[COD_CAP_DIR_COMMERCIALE]
			,[COD_PROVINCIA_DIR_COMMERCIALE]
			,[COD_AREA]
			,[DES_AREA]
			,[NOM_INDIRIZZO_AREA]
			,[NOM_COMUNE_AREA]
			,[COD_CAP_AREA]
			,[COD_PROVINCIA_AREA]
			,[COD_ABI]
			,[COD_FILIALE_UFFICIO]
			,[DES_FILIALE_UFFICIO]
			,[COD_DW_FILIALE]
			,[NOM_INDIRIZZO_FILIALE_UFFICIO]
			,[NOM_LOCALITA_FILIALE_UFFICIO]
			,[NOM_COMUNE_FILIALE_UFFICIO]
			,[COD_CAP_FILIALE_UFFICIO]
			,[NOM_PROVINCIA_FILIALE_UFFICIO]
			,[GEO_COORDINATA_X]
			,[GEO_COORDINATA_Y]
			,[COD_CLASSE_FILIALE_UFFICIO]
			,[DES_CLASSE_FILIALE_UFFICIO]
			,[TERRITORIO]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[ANNOMESE]
			,[src].[COD_DIREZIONE_REGIONALE]
			,[src].[DES_DIREZIONE_REGIONALE]
			,[src].[NOM_INDIRIZZO_DIR_REGIONALE]
			,[src].[NOM_COMUNE_DIR_REGIONALE]
			,[src].[COD_CAP_DIR_REGIONALE]
			,[src].[COD_PROVINCIA_DIR_REGIONALE]
			,[src].[COD_DIREZIONE_COMMERCIALE]
			,[src].[DES_DIREZIONE_COMMERCIALE]
			,[src].[NOM_INDIRIZZO_DIR_COMMERCIALE]
			,[src].[NOM_COMUNE_DIR_COMMERCIALE]
			,[src].[COD_CAP_DIR_COMMERCIALE]
			,[src].[COD_PROVINCIA_DIR_COMMERCIALE]
			,[src].[COD_AREA]
			,[src].[DES_AREA]
			,[src].[NOM_INDIRIZZO_AREA]
			,[src].[NOM_COMUNE_AREA]
			,[src].[COD_CAP_AREA]
			,[src].[COD_PROVINCIA_AREA]
			,[src].[COD_ABI]
			,[src].[COD_FILIALE_UFFICIO]
			,[src].[DES_FILIALE_UFFICIO]
			,[src].[COD_DW_FILIALE]
			,[src].[NOM_INDIRIZZO_FILIALE_UFFICIO]
			,[src].[NOM_LOCALITA_FILIALE_UFFICIO]
			,[src].[NOM_COMUNE_FILIALE_UFFICIO]
			,[src].[COD_CAP_FILIALE_UFFICIO]
			,[src].[NOM_PROVINCIA_FILIALE_UFFICIO]
			,[src].[GEO_COORDINATA_X]
			,[src].[GEO_COORDINATA_Y]
			,[src].[COD_CLASSE_FILIALE_UFFICIO]
			,[src].[DES_CLASSE_FILIALE_UFFICIO]
			,[src].[TERRITORIO]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[ANNOMESE] = [src].[ANNOMESE]
			,[COD_DIREZIONE_REGIONALE] = [src].[COD_DIREZIONE_REGIONALE]
			,[DES_DIREZIONE_REGIONALE] = [src].[DES_DIREZIONE_REGIONALE]
			,[NOM_INDIRIZZO_DIR_REGIONALE] = [src].[NOM_INDIRIZZO_DIR_REGIONALE]
			,[NOM_COMUNE_DIR_REGIONALE] = [src].[NOM_COMUNE_DIR_REGIONALE]
			,[COD_CAP_DIR_REGIONALE] = [src].[COD_CAP_DIR_REGIONALE]
			,[COD_PROVINCIA_DIR_REGIONALE] = [src].[COD_PROVINCIA_DIR_REGIONALE]
			,[COD_DIREZIONE_COMMERCIALE] = [src].[COD_DIREZIONE_COMMERCIALE]
			,[DES_DIREZIONE_COMMERCIALE] = [src].[DES_DIREZIONE_COMMERCIALE]
			,[NOM_INDIRIZZO_DIR_COMMERCIALE] = [src].[NOM_INDIRIZZO_DIR_COMMERCIALE]
			,[NOM_COMUNE_DIR_COMMERCIALE] = [src].[NOM_COMUNE_DIR_COMMERCIALE]
			,[COD_CAP_DIR_COMMERCIALE] = [src].[COD_CAP_DIR_COMMERCIALE]
			,[COD_PROVINCIA_DIR_COMMERCIALE] = [src].[COD_PROVINCIA_DIR_COMMERCIALE]
			,[COD_AREA] = [src].[COD_AREA]
			,[DES_AREA] = [src].[DES_AREA]
			,[NOM_INDIRIZZO_AREA] = [src].[NOM_INDIRIZZO_AREA]
			,[NOM_COMUNE_AREA] = [src].[NOM_COMUNE_AREA]
			,[COD_CAP_AREA] = [src].[COD_CAP_AREA]
			,[COD_PROVINCIA_AREA] = [src].[COD_PROVINCIA_AREA]
			,[COD_ABI] = [src].[COD_ABI]
			,[COD_FILIALE_UFFICIO] = [src].[COD_FILIALE_UFFICIO]
			,[DES_FILIALE_UFFICIO] = [src].[DES_FILIALE_UFFICIO]
			,[COD_DW_FILIALE] = [src].[COD_DW_FILIALE]
			,[NOM_INDIRIZZO_FILIALE_UFFICIO] = [src].[NOM_INDIRIZZO_FILIALE_UFFICIO]
			,[NOM_LOCALITA_FILIALE_UFFICIO] = [src].[NOM_LOCALITA_FILIALE_UFFICIO]
			,[NOM_COMUNE_FILIALE_UFFICIO] = [src].[NOM_COMUNE_FILIALE_UFFICIO]
			,[COD_CAP_FILIALE_UFFICIO] = [src].[COD_CAP_FILIALE_UFFICIO]
			,[NOM_PROVINCIA_FILIALE_UFFICIO] = [src].[NOM_PROVINCIA_FILIALE_UFFICIO]
			,[GEO_COORDINATA_X] = [src].[GEO_COORDINATA_X]
			,[GEO_COORDINATA_Y] = [src].[GEO_COORDINATA_Y]
			,[COD_CLASSE_FILIALE_UFFICIO] = [src].[COD_CLASSE_FILIALE_UFFICIO]
			,[DES_CLASSE_FILIALE_UFFICIO] = [src].[DES_CLASSE_FILIALE_UFFICIO]
			,[TERRITORIO] = [src].[TERRITORIO]
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