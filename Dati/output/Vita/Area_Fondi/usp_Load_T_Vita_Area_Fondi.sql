/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Area_Fondi] alla tabella [L1].[T_Vita_Area_Fondi].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Area_Fondi]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Area_Fondi]
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
		
		UPDATE [L0].[T_Vita_Area_Fondi]
		SET [BitMask_Scarti] = 0 
		; 
 

--		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
--		;
--		UPDATE [sn]
--		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--		FROM [L0].[T_Vita_Area_Fondi] [sn]
--		JOIN ( SELECT 
--			[sn].[COD_ABI]
--			,[sn].[COD_CONTRATTO]
--		FROM [L0].[T_Vita_Area_Fondi] [sn]
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
	FROM [L0].[T_Vita_Area_Fondi] [sn]
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
	FROM [L0].[T_Vita_Area_Fondi] [sn]
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
		FROM [L0].[T_Vita_Area_Fondi] [r]
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
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Vita_Area_Fondi](
		[COD_ABI]
		,[COD_CONTRATTO]
		,[COD_PRODOTTO]
		,[DES_PRODOTTO]
		,[DES_MACROAREA]
		,[COD_COMPONENTE]
		,[DES_COMPONENTE]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[COD_ABI]
		,[COD_CONTRATTO]
		,[COD_PRODOTTO]
		,[DES_PRODOTTO]
		,[DES_MACROAREA]
		,[COD_COMPONENTE]
		,[DES_COMPONENTE]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Area_Fondi]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Area_Fondi] AS dst
	USING 
		( SELECT
			[COD_ABI]
			,[COD_CONTRATTO]
			,[COD_PRODOTTO]
			,[DES_PRODOTTO]
			,[DES_MACROAREA]
			,[COD_COMPONENTE]
			,[DES_COMPONENTE]
			,[Row_Id]
		FROM [L0].[T_Vita_Area_Fondi]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			on [src].[COD_ABI] = [dst].[COD_ABI]
			AND [src].[COD_CONTRATTO] = [dst].[COD_CONTRATTO]
	WHEN not matched THEN INSERT (
			[COD_ABI]
			,[COD_CONTRATTO]
			,[COD_PRODOTTO]
			,[DES_PRODOTTO]
			,[DES_MACROAREA]
			,[COD_COMPONENTE]
			,[DES_COMPONENTE]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[COD_ABI]
			,[src].[COD_CONTRATTO]
			,[src].[COD_PRODOTTO]
			,[src].[DES_PRODOTTO]
			,[src].[DES_MACROAREA]
			,[src].[COD_COMPONENTE]
			,[src].[DES_COMPONENTE]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[COD_PRODOTTO] = [src].[COD_PRODOTTO]
			,[DES_PRODOTTO] = [src].[DES_PRODOTTO]
			,[DES_MACROAREA] = [src].[DES_MACROAREA]
			,[COD_COMPONENTE] = [src].[COD_COMPONENTE]
			,[DES_COMPONENTE] = [src].[DES_COMPONENTE]
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