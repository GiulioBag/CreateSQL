/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] alla tabella [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI]
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
		
		UPDATE [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI]
		SET [BitMask_Scarti] = 0 
		; 
 

----		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
----		;
----		UPDATE [sn]
----		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
----		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] [sn]
----		JOIN ( SELECT 
----			Inserire qui le eventuali chiavi nella forma [sn].[key]
----		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] [sn]
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
--	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] [sn]
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
--	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] [sn]
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
--		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] [r]
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
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_APERTURA_CONTRATTO] is null OR [sn].[DAT_APERTURA_CONTRATTO] = '')
			--OR ([sn].[DAT_CHIUSURA_CONTRATTO] is null OR [sn].[DAT_CHIUSURA_CONTRATTO] = '')
			--OR ([sn].[DAT_PRIMO_CONFERIMENTO_PRD] is null OR [sn].[DAT_PRIMO_CONFERIMENTO_PRD] = '')
			--OR ([sn].[DAT_ULTIMO_CONFERIMENTO_PRD] is null OR [sn].[DAT_ULTIMO_CONFERIMENTO_PRD] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_APERTURA_CONTRATTO] is not null AND [sn].[DAT_APERTURA_CONTRATTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_APERTURA_CONTRATTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_CHIUSURA_CONTRATTO] is not null AND [sn].[DAT_CHIUSURA_CONTRATTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_CHIUSURA_CONTRATTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_PRIMO_CONFERIMENTO_PRD] is not null AND [sn].[DAT_PRIMO_CONFERIMENTO_PRD]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_PRIMO_CONFERIMENTO_PRD], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_ULTIMO_CONFERIMENTO_PRD] is not null AND [sn].[DAT_ULTIMO_CONFERIMENTO_PRD]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_ULTIMO_CONFERIMENTO_PRD], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[IMP_INIZIALE_INVESTITO] is null )
			--OR ([sn].[IMP_SOGLIA_INIZIO_CONFERIMENTO] is null )
			--OR ([sn].[IMP_SOGLIA_INIZIALE_SWITCH] is null )
			--OR ([sn].[IMP_SOGLIA_VARZN_CONFRMNT] is null )
		)
	; 
 

	--Esclusione numeric non Validi
	SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[IMP_INIZIALE_INVESTITO] is NOT null AND TRY_CAST(REPLACE([IMP_INIZIALE_INVESTITO], ',', '.') as NUMERIC (23,3)) IS  NULL))
			OR ([sn].[IMP_SOGLIA_INIZIO_CONFERIMENTO] is NOT null AND TRY_CAST(REPLACE([IMP_SOGLIA_INIZIO_CONFERIMENTO], ',', '.') as NUMERIC (23,3)) IS  NULL))
			OR ([sn].[IMP_SOGLIA_INIZIALE_SWITCH] is NOT null AND TRY_CAST(REPLACE([IMP_SOGLIA_INIZIALE_SWITCH], ',', '.') as NUMERIC (23,3)) IS  NULL))
			OR ([sn].[IMP_SOGLIA_VARZN_CONFRMNT] is NOT null AND TRY_CAST(REPLACE([IMP_SOGLIA_VARZN_CONFRMNT], ',', '.') as NUMERIC (29,3)) IS  NULL))
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI](
		[COD_ABI]
		,[COD_CONTRATTO]
		,[COD_MANDATO]
		,[COD_FILIALE_RAPPORTO]
		,[COD_NDG_CONTRATTUALE]
		,[DAT_APERTURA_CONTRATTO]
		,[DAT_CHIUSURA_CONTRATTO]
		,[COD_STATO_CONTRATTO_GP]
		,[DES_STATO_CONTRATTO_GP]
		,[COD_SGR]
		,[DES_SOCIETA_GESTIONE_RISPARMIO]
		,[IMP_INIZIALE_INVESTITO]
		,[IMP_SOGLIA_INIZIO_CONFERIMENTO]
		,[IMP_SOGLIA_INIZIALE_SWITCH]
		,[IMP_SOGLIA_VARZN_CONFRMNT]
		,[DAT_PRIMO_CONFERIMENTO_PRD]
		,[DAT_ULTIMO_CONFERIMENTO_PRD]
		,[COD_LINEA_PRODOTTO]
		,[DES_LINEA_PRODOTTO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[COD_ABI]
		,[COD_CONTRATTO]
		,[COD_MANDATO]
		,[COD_FILIALE_RAPPORTO]
		,[COD_NDG_CONTRATTUALE]
		,[DAT_APERTURA_CONTRATTO]
		,[DAT_CHIUSURA_CONTRATTO]
		,[COD_STATO_CONTRATTO_GP]
		,[DES_STATO_CONTRATTO_GP]
		,[COD_SGR]
		,[DES_SOCIETA_GESTIONE_RISPARMIO]
		,[IMP_INIZIALE_INVESTITO]
		,[IMP_SOGLIA_INIZIO_CONFERIMENTO]
		,[IMP_SOGLIA_INIZIALE_SWITCH]
		,[IMP_SOGLIA_VARZN_CONFRMNT]
		,[DAT_PRIMO_CONFERIMENTO_PRD]
		,[DAT_ULTIMO_CONFERIMENTO_PRD]
		,[COD_LINEA_PRODOTTO]
		,[DES_LINEA_PRODOTTO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI] AS dst
	USING 
		( SELECT
			[COD_ABI]
			,[COD_CONTRATTO]
			,[COD_MANDATO]
			,[COD_FILIALE_RAPPORTO]
			,[COD_NDG_CONTRATTUALE]
			,TRY_CONVERT (date, stuff(stuff([DAT_APERTURA_CONTRATTO], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_CHIUSURA_CONTRATTO], 6, 0, ' '), 3, 0, ' '), 106)
			,[COD_STATO_CONTRATTO_GP]
			,[DES_STATO_CONTRATTO_GP]
			,[COD_SGR]
			,[DES_SOCIETA_GESTIONE_RISPARMIO]
			,TRY_CAST(REPLACE([IMP_INIZIALE_INVESTITO], ',', '.') as NUMERIC (23,3)) 
			,TRY_CAST(REPLACE([IMP_SOGLIA_INIZIO_CONFERIMENTO], ',', '.') as NUMERIC (23,3)) 
			,TRY_CAST(REPLACE([IMP_SOGLIA_INIZIALE_SWITCH], ',', '.') as NUMERIC (23,3)) 
			,TRY_CAST(REPLACE([IMP_SOGLIA_VARZN_CONFRMNT], ',', '.') as NUMERIC (29,3)) 
			,TRY_CONVERT (date, stuff(stuff([DAT_PRIMO_CONFERIMENTO_PRD], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_ULTIMO_CONFERIMENTO_PRD], 6, 0, ' '), 3, 0, ' '), 106)
			,[COD_LINEA_PRODOTTO]
			,[DES_LINEA_PRODOTTO]
			,[Row_Id]
		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_RAPPORTI_MOVIMENTI]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			-- Inserire qui le chiavi nella forma: on [src].[key] = [dst].[key]  
			 -- AND [src].[key] = [dst].[key]
	WHEN not matched THEN INSERT (
			[COD_ABI]
			,[COD_CONTRATTO]
			,[COD_MANDATO]
			,[COD_FILIALE_RAPPORTO]
			,[COD_NDG_CONTRATTUALE]
			,[DAT_APERTURA_CONTRATTO]
			,[DAT_CHIUSURA_CONTRATTO]
			,[COD_STATO_CONTRATTO_GP]
			,[DES_STATO_CONTRATTO_GP]
			,[COD_SGR]
			,[DES_SOCIETA_GESTIONE_RISPARMIO]
			,[IMP_INIZIALE_INVESTITO]
			,[IMP_SOGLIA_INIZIO_CONFERIMENTO]
			,[IMP_SOGLIA_INIZIALE_SWITCH]
			,[IMP_SOGLIA_VARZN_CONFRMNT]
			,[DAT_PRIMO_CONFERIMENTO_PRD]
			,[DAT_ULTIMO_CONFERIMENTO_PRD]
			,[COD_LINEA_PRODOTTO]
			,[DES_LINEA_PRODOTTO]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[COD_ABI]
			,[src].[COD_CONTRATTO]
			,[src].[COD_MANDATO]
			,[src].[COD_FILIALE_RAPPORTO]
			,[src].[COD_NDG_CONTRATTUALE]
			,[src].[DAT_APERTURA_CONTRATTO]
			,[src].[DAT_CHIUSURA_CONTRATTO]
			,[src].[COD_STATO_CONTRATTO_GP]
			,[src].[DES_STATO_CONTRATTO_GP]
			,[src].[COD_SGR]
			,[src].[DES_SOCIETA_GESTIONE_RISPARMIO]
			,[src].[IMP_INIZIALE_INVESTITO]
			,[src].[IMP_SOGLIA_INIZIO_CONFERIMENTO]
			,[src].[IMP_SOGLIA_INIZIALE_SWITCH]
			,[src].[IMP_SOGLIA_VARZN_CONFRMNT]
			,[src].[DAT_PRIMO_CONFERIMENTO_PRD]
			,[src].[DAT_ULTIMO_CONFERIMENTO_PRD]
			,[src].[COD_LINEA_PRODOTTO]
			,[src].[DES_LINEA_PRODOTTO]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[COD_ABI] = [src].[COD_ABI]
			,[COD_CONTRATTO] = [src].[COD_CONTRATTO]
			,[COD_MANDATO] = [src].[COD_MANDATO]
			,[COD_FILIALE_RAPPORTO] = [src].[COD_FILIALE_RAPPORTO]
			,[COD_NDG_CONTRATTUALE] = [src].[COD_NDG_CONTRATTUALE]
			,[DAT_APERTURA_CONTRATTO] = [src].[DAT_APERTURA_CONTRATTO]
			,[DAT_CHIUSURA_CONTRATTO] = [src].[DAT_CHIUSURA_CONTRATTO]
			,[COD_STATO_CONTRATTO_GP] = [src].[COD_STATO_CONTRATTO_GP]
			,[DES_STATO_CONTRATTO_GP] = [src].[DES_STATO_CONTRATTO_GP]
			,[COD_SGR] = [src].[COD_SGR]
			,[DES_SOCIETA_GESTIONE_RISPARMIO] = [src].[DES_SOCIETA_GESTIONE_RISPARMIO]
			,[IMP_INIZIALE_INVESTITO] = [src].[IMP_INIZIALE_INVESTITO]
			,[IMP_SOGLIA_INIZIO_CONFERIMENTO] = [src].[IMP_SOGLIA_INIZIO_CONFERIMENTO]
			,[IMP_SOGLIA_INIZIALE_SWITCH] = [src].[IMP_SOGLIA_INIZIALE_SWITCH]
			,[IMP_SOGLIA_VARZN_CONFRMNT] = [src].[IMP_SOGLIA_VARZN_CONFRMNT]
			,[DAT_PRIMO_CONFERIMENTO_PRD] = [src].[DAT_PRIMO_CONFERIMENTO_PRD]
			,[DAT_ULTIMO_CONFERIMENTO_PRD] = [src].[DAT_ULTIMO_CONFERIMENTO_PRD]
			,[COD_LINEA_PRODOTTO] = [src].[COD_LINEA_PRODOTTO]
			,[DES_LINEA_PRODOTTO] = [src].[DES_LINEA_PRODOTTO]
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