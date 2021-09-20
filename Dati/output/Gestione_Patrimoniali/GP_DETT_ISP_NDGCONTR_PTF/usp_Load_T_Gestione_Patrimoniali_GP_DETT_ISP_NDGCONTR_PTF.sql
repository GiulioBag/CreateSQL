/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF] alla tabella [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF]
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
		
		UPDATE [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF]
		SET [BitMask_Scarti] = 0 
		; 
 

----		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
----		;
----		UPDATE [sn]
----		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
----		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF] [sn]
----		JOIN ( SELECT 
----			Inserire qui le eventuali chiavi nella forma [sn].[key]
----		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF] [sn]
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
--	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF] [sn]
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
--	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF] [sn]
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
--		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF] [r]
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
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_INIZIO_ASSOCIAZIONE_PTF] is null OR [sn].[DAT_INIZIO_ASSOCIAZIONE_PTF] = '')
			--OR ([sn].[DAT_FINE_ASSOCIAZIONE_PTF] is null OR [sn].[DAT_FINE_ASSOCIAZIONE_PTF] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_INIZIO_ASSOCIAZIONE_PTF] is not null AND [sn].[DAT_INIZIO_ASSOCIAZIONE_PTF]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_INIZIO_ASSOCIAZIONE_PTF], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_FINE_ASSOCIAZIONE_PTF] is not null AND [sn].[DAT_FINE_ASSOCIAZIONE_PTF]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_FINE_ASSOCIAZIONE_PTF], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF](
		[COD_ABI]
		,[COD_NDG_CONTRATTUALE]
		,[COD_MANDATO]
		,[COD_FILIALE_NDG_CONTRATTO]
		,[COD_FILIALE_PORTAFOGLIO]
		,[COD_PORTAFOGLIO]
		,[COD_TIPO_PORTAFOGLIO]
		,[DAT_INIZIO_ASSOCIAZIONE_PTF]
		,[DAT_FINE_ASSOCIAZIONE_PTF]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[COD_ABI]
		,[COD_NDG_CONTRATTUALE]
		,[COD_MANDATO]
		,[COD_FILIALE_NDG_CONTRATTO]
		,[COD_FILIALE_PORTAFOGLIO]
		,[COD_PORTAFOGLIO]
		,[COD_TIPO_PORTAFOGLIO]
		,[DAT_INIZIO_ASSOCIAZIONE_PTF]
		,[DAT_FINE_ASSOCIAZIONE_PTF]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF] AS dst
	USING 
		( SELECT
			[COD_ABI]
			,[COD_NDG_CONTRATTUALE]
			,[COD_MANDATO]
			,[COD_FILIALE_NDG_CONTRATTO]
			,[COD_FILIALE_PORTAFOGLIO]
			,[COD_PORTAFOGLIO]
			,[COD_TIPO_PORTAFOGLIO]
			,TRY_CONVERT (date, stuff(stuff([DAT_INIZIO_ASSOCIAZIONE_PTF], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_FINE_ASSOCIAZIONE_PTF], 6, 0, ' '), 3, 0, ' '), 106)
			,[Row_Id]
		FROM [L0].[T_Gestione_Patrimoniali_GP_DETT_ISP_NDGCONTR_PTF]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			-- Inserire qui le chiavi nella forma: on [src].[key] = [dst].[key]  
			 -- AND [src].[key] = [dst].[key]
	WHEN not matched THEN INSERT (
			[COD_ABI]
			,[COD_NDG_CONTRATTUALE]
			,[COD_MANDATO]
			,[COD_FILIALE_NDG_CONTRATTO]
			,[COD_FILIALE_PORTAFOGLIO]
			,[COD_PORTAFOGLIO]
			,[COD_TIPO_PORTAFOGLIO]
			,[DAT_INIZIO_ASSOCIAZIONE_PTF]
			,[DAT_FINE_ASSOCIAZIONE_PTF]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[COD_ABI]
			,[src].[COD_NDG_CONTRATTUALE]
			,[src].[COD_MANDATO]
			,[src].[COD_FILIALE_NDG_CONTRATTO]
			,[src].[COD_FILIALE_PORTAFOGLIO]
			,[src].[COD_PORTAFOGLIO]
			,[src].[COD_TIPO_PORTAFOGLIO]
			,[src].[DAT_INIZIO_ASSOCIAZIONE_PTF]
			,[src].[DAT_FINE_ASSOCIAZIONE_PTF]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[COD_ABI] = [src].[COD_ABI]
			,[COD_NDG_CONTRATTUALE] = [src].[COD_NDG_CONTRATTUALE]
			,[COD_MANDATO] = [src].[COD_MANDATO]
			,[COD_FILIALE_NDG_CONTRATTO] = [src].[COD_FILIALE_NDG_CONTRATTO]
			,[COD_FILIALE_PORTAFOGLIO] = [src].[COD_FILIALE_PORTAFOGLIO]
			,[COD_PORTAFOGLIO] = [src].[COD_PORTAFOGLIO]
			,[COD_TIPO_PORTAFOGLIO] = [src].[COD_TIPO_PORTAFOGLIO]
			,[DAT_INIZIO_ASSOCIAZIONE_PTF] = [src].[DAT_INIZIO_ASSOCIAZIONE_PTF]
			,[DAT_FINE_ASSOCIAZIONE_PTF] = [src].[DAT_FINE_ASSOCIAZIONE_PTF]
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