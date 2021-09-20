/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Anagrafica_Clienti] alla tabella [L1].[T_Vita_Anagrafica_Clienti].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Anagrafica_Clienti]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Anagrafica_Clienti]
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
		
		UPDATE [L0].[T_Vita_Anagrafica_Clienti]
		SET [BitMask_Scarti] = 0 
		; 
 

--		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
--		;
--		UPDATE [sn]
--		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--		FROM [L0].[T_Vita_Anagrafica_Clienti] [sn]
--		JOIN ( SELECT 
--			[sn].[COD_ABI]
--			,[sn].[COD_NDG]
--		FROM [L0].[T_Vita_Anagrafica_Clienti] [sn]
--		WHERE [Exec_Id] = @Exec_Id
--			[sn].[COD_ABI] <> '' AND [sn].[COD_ABI] IS NOT NULL
--			AND [sn].[COD_NDG] <> '' AND [sn].[COD_NDG] IS NOT NULL
--		GROUP BY
--			[sn].[COD_ABI]
--			,[sn].[COD_NDG]
--		HAVING COUNT(*) > 1
--	) [sn2]
--	on
--		[sn].[COD_ABI] = [sn2].[COD_ABI]
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
	FROM [L0].[T_Vita_Anagrafica_Clienti] [sn]
	CROSS JOIN[L0_SCARTI].[T_Desc_Scarti][scarti]
	where[Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_KEY' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			([sn].[COD_ABI] is null OR [sn].[COD_ABI] = '')
			OR ([sn].[COD_NDG] is null OR [sn].[COD_NDG] = '')
		)
	; 
 

	--Viene preso solamente una riga tra le N righe duplicate.
	--Viene presa la prima riga del file (Ordinament o per Row_Id ASC)
	SET @Step = '2.3 Scarti: Applicazione criterio di scarto GET_ONE_DUPLICATE_KEY'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Clienti] [sn]
	JOIN (
		SELECT 
			[r].[COD_ABI]
			,[r].[COD_NDG]
			,[r].[Row_Id]
			,[r].[Exec_Id]
			,ROW_NUMBER() OVER(
			PARTIOTION BY
					[r].[COD_ABI]
					,[r].[COD_NDG]
				ORDER BY
					[r].[Row_Id] ASC
					,[r].[Exec_Id] ASC]
		) as [rn]
		FROM [L0].[T_Vita_Anagrafica_Clienti] [r]
		WHERE [Exec_Id] = @Exec_Id
	) [sn2]
		on
			[sn].[COD_ABI] = [sn2].[COD_ABI]
			AND [sn].[COD_NDG] = [sn2].[COD_NDG]
			AND [sn].[Row_Id] = [sn2].[Row_Id]
			AND [sn].[Exec_Id] = [sn2].[Exec_Id]
	[sn].[COD_ABI] = [sn2].[COD_ABI]
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
	FROM [L0].[T_Vita_Anagrafica_Clienti] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_NASCITA] is null OR [sn].[DAT_NASCITA] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Clienti] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_NASCITA] is not null AND [sn].[DAT_NASCITA]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_NASCITA], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Clienti] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[NUM_ANNO_MESE] is null )
			--OR ([sn].[FLAG_FIX_PROF_INVST] is null )
			--OR ([sn].[FLAG_FIX_ESPR_INVST] is null )
		)
	; 
 

	--Esclusione numeric non Validi
	SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Clienti] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[NUM_ANNO_MESE] is NOT null AND TRY_CAST([NUM_ANNO_MESE] as int) IS  NULL))
			OR ([sn].[FLAG_FIX_PROF_INVST] is NOT null AND TRY_CAST([FLAG_FIX_PROF_INVST] as bit) IS  NULL))
			OR ([sn].[FLAG_FIX_ESPR_INVST] is NOT null AND TRY_CAST([FLAG_FIX_ESPR_INVST] as bit) IS  NULL))
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Vita_Anagrafica_Clienti](
		[NUM_ANNO_MESE]
		,[COD_SUPERNSG]
		,[COD_ABI]
		,[COD_NDG]
		,[COD_FISCALE]
		,[COD_PARTITA_IVA]
		,[COD_FORMA_GIURIDICA]
		,[DAT_NASCITA]
		,[COD_PROFILO_INVESTITORE]
		,[DES_PROFILO_INVESTITORE]
		,[COD_ESPERIENZA_INVESTIMENTO]
		,[DES_ESPERIENZA_INVESTIMENTO]
		,[COD_ABI_FIL_PTF]
		,[COD_FILIALE_PORTAFOGLIO]
		,[COD_PORTAFOGLIO]
		,[COD_TIPO_PORTAFOGLIO]
		,[COD_FILIALE_RIFERIMENTO]
		,[FLAG_FIX_PROF_INVST]
		,[FLAG_FIX_ESPR_INVST]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[NUM_ANNO_MESE]
		,[COD_SUPERNSG]
		,[COD_ABI]
		,[COD_NDG]
		,[COD_FISCALE]
		,[COD_PARTITA_IVA]
		,[COD_FORMA_GIURIDICA]
		,[DAT_NASCITA]
		,[COD_PROFILO_INVESTITORE]
		,[DES_PROFILO_INVESTITORE]
		,[COD_ESPERIENZA_INVESTIMENTO]
		,[DES_ESPERIENZA_INVESTIMENTO]
		,[COD_ABI_FIL_PTF]
		,[COD_FILIALE_PORTAFOGLIO]
		,[COD_PORTAFOGLIO]
		,[COD_TIPO_PORTAFOGLIO]
		,[COD_FILIALE_RIFERIMENTO]
		,[FLAG_FIX_PROF_INVST]
		,[FLAG_FIX_ESPR_INVST]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Anagrafica_Clienti]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Anagrafica_Clienti] AS dst
	USING 
		( SELECT
			TRY_CAST([NUM_ANNO_MESE] as int) 
			,[COD_SUPERNSG]
			,[COD_ABI]
			,[COD_NDG]
			,[COD_FISCALE]
			,[COD_PARTITA_IVA]
			,[COD_FORMA_GIURIDICA]
			,TRY_CONVERT (date, stuff(stuff([DAT_NASCITA], 6, 0, ' '), 3, 0, ' '), 106)
			,[COD_PROFILO_INVESTITORE]
			,[DES_PROFILO_INVESTITORE]
			,[COD_ESPERIENZA_INVESTIMENTO]
			,[DES_ESPERIENZA_INVESTIMENTO]
			,[COD_ABI_FIL_PTF]
			,[COD_FILIALE_PORTAFOGLIO]
			,[COD_PORTAFOGLIO]
			,[COD_TIPO_PORTAFOGLIO]
			,[COD_FILIALE_RIFERIMENTO]
			,TRY_CAST([FLAG_FIX_PROF_INVST] as bit) 
			,TRY_CAST([FLAG_FIX_ESPR_INVST] as bit) 
			,[Row_Id]
		FROM [L0].[T_Vita_Anagrafica_Clienti]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			on [src].[COD_ABI] = [dst].[COD_ABI]
			AND [src].[COD_NDG] = [dst].[COD_NDG]
	WHEN not matched THEN INSERT (
			[NUM_ANNO_MESE]
			,[COD_SUPERNSG]
			,[COD_ABI]
			,[COD_NDG]
			,[COD_FISCALE]
			,[COD_PARTITA_IVA]
			,[COD_FORMA_GIURIDICA]
			,[DAT_NASCITA]
			,[COD_PROFILO_INVESTITORE]
			,[DES_PROFILO_INVESTITORE]
			,[COD_ESPERIENZA_INVESTIMENTO]
			,[DES_ESPERIENZA_INVESTIMENTO]
			,[COD_ABI_FIL_PTF]
			,[COD_FILIALE_PORTAFOGLIO]
			,[COD_PORTAFOGLIO]
			,[COD_TIPO_PORTAFOGLIO]
			,[COD_FILIALE_RIFERIMENTO]
			,[FLAG_FIX_PROF_INVST]
			,[FLAG_FIX_ESPR_INVST]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[NUM_ANNO_MESE]
			,[src].[COD_SUPERNSG]
			,[src].[COD_ABI]
			,[src].[COD_NDG]
			,[src].[COD_FISCALE]
			,[src].[COD_PARTITA_IVA]
			,[src].[COD_FORMA_GIURIDICA]
			,[src].[DAT_NASCITA]
			,[src].[COD_PROFILO_INVESTITORE]
			,[src].[DES_PROFILO_INVESTITORE]
			,[src].[COD_ESPERIENZA_INVESTIMENTO]
			,[src].[DES_ESPERIENZA_INVESTIMENTO]
			,[src].[COD_ABI_FIL_PTF]
			,[src].[COD_FILIALE_PORTAFOGLIO]
			,[src].[COD_PORTAFOGLIO]
			,[src].[COD_TIPO_PORTAFOGLIO]
			,[src].[COD_FILIALE_RIFERIMENTO]
			,[src].[FLAG_FIX_PROF_INVST]
			,[src].[FLAG_FIX_ESPR_INVST]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[NUM_ANNO_MESE] = [src].[NUM_ANNO_MESE]
			,[COD_SUPERNSG] = [src].[COD_SUPERNSG]
			,[COD_FISCALE] = [src].[COD_FISCALE]
			,[COD_PARTITA_IVA] = [src].[COD_PARTITA_IVA]
			,[COD_FORMA_GIURIDICA] = [src].[COD_FORMA_GIURIDICA]
			,[DAT_NASCITA] = [src].[DAT_NASCITA]
			,[COD_PROFILO_INVESTITORE] = [src].[COD_PROFILO_INVESTITORE]
			,[DES_PROFILO_INVESTITORE] = [src].[DES_PROFILO_INVESTITORE]
			,[COD_ESPERIENZA_INVESTIMENTO] = [src].[COD_ESPERIENZA_INVESTIMENTO]
			,[DES_ESPERIENZA_INVESTIMENTO] = [src].[DES_ESPERIENZA_INVESTIMENTO]
			,[COD_ABI_FIL_PTF] = [src].[COD_ABI_FIL_PTF]
			,[COD_FILIALE_PORTAFOGLIO] = [src].[COD_FILIALE_PORTAFOGLIO]
			,[COD_PORTAFOGLIO] = [src].[COD_PORTAFOGLIO]
			,[COD_TIPO_PORTAFOGLIO] = [src].[COD_TIPO_PORTAFOGLIO]
			,[COD_FILIALE_RIFERIMENTO] = [src].[COD_FILIALE_RIFERIMENTO]
			,[FLAG_FIX_PROF_INVST] = [src].[FLAG_FIX_PROF_INVST]
			,[FLAG_FIX_ESPR_INVST] = [src].[FLAG_FIX_ESPR_INVST]
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