/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Polizze_Prodotto] alla tabella [L1].[T_Vita_Polizze_Prodotto].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Polizze_Prodotto]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Polizze_Prodotto]
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
		
		UPDATE [L0].[T_Vita_Polizze_Prodotto]
		SET [BitMask_Scarti] = 0 
		; 
 

--		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
--		;
--		UPDATE [sn]
--		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--		FROM [L0].[T_Vita_Polizze_Prodotto] [sn]
--		JOIN ( SELECT 
--			[sn].[COD_ABI]
--			,[sn].[COD_COMPAGNIA]
--			,[sn].[COD_PRODOTTO_INTERNO]
--			,[sn].[COD_PRODOTTO]
--		FROM [L0].[T_Vita_Polizze_Prodotto] [sn]
--		WHERE [Exec_Id] = @Exec_Id
--			[sn].[COD_ABI] <> '' AND [sn].[COD_ABI] IS NOT NULL
--			AND [sn].[COD_COMPAGNIA] <> '' AND [sn].[COD_COMPAGNIA] IS NOT NULL
--			AND [sn].[COD_PRODOTTO_INTERNO] <> '' AND [sn].[COD_PRODOTTO_INTERNO] IS NOT NULL
--			AND [sn].[COD_PRODOTTO] <> '' AND [sn].[COD_PRODOTTO] IS NOT NULL
--		GROUP BY
--			[sn].[COD_ABI]
--			,[sn].[COD_COMPAGNIA]
--			,[sn].[COD_PRODOTTO_INTERNO]
--			,[sn].[COD_PRODOTTO]
--		HAVING COUNT(*) > 1
--	) [sn2]
--	on
--		[sn].[COD_ABI] = [sn2].[COD_ABI]
--		AND [sn].[COD_COMPAGNIA] = [sn2].[COD_COMPAGNIA]
--		AND [sn].[COD_PRODOTTO_INTERNO] = [sn2].[COD_PRODOTTO_INTERNO]
--		AND [sn].[COD_PRODOTTO] = [sn2].[COD_PRODOTTO]
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
	FROM [L0].[T_Vita_Polizze_Prodotto] [sn]
	CROSS JOIN[L0_SCARTI].[T_Desc_Scarti][scarti]
	where[Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_KEY' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			([sn].[COD_ABI] is null OR [sn].[COD_ABI] = '')
			OR ([sn].[COD_COMPAGNIA] is null OR [sn].[COD_COMPAGNIA] = '')
			OR ([sn].[COD_PRODOTTO_INTERNO] is null OR [sn].[COD_PRODOTTO_INTERNO] = '')
			OR ([sn].[COD_PRODOTTO] is null OR [sn].[COD_PRODOTTO] = '')
		)
	; 
 

	--Viene preso solamente una riga tra le N righe duplicate.
	--Viene presa la prima riga del file (Ordinament o per Row_Id ASC)
	SET @Step = '2.3 Scarti: Applicazione criterio di scarto GET_ONE_DUPLICATE_KEY'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Polizze_Prodotto] [sn]
	JOIN (
		SELECT 
			[r].[COD_ABI]
			,[r].[COD_COMPAGNIA]
			,[r].[COD_PRODOTTO_INTERNO]
			,[r].[COD_PRODOTTO]
			,[r].[Row_Id]
			,[r].[Exec_Id]
			,ROW_NUMBER() OVER(
			PARTIOTION BY
					[r].[COD_ABI]
					,[r].[COD_COMPAGNIA]
					,[r].[COD_PRODOTTO_INTERNO]
					,[r].[COD_PRODOTTO]
				ORDER BY
					[r].[Row_Id] ASC
					,[r].[Exec_Id] ASC]
		) as [rn]
		FROM [L0].[T_Vita_Polizze_Prodotto] [r]
		WHERE [Exec_Id] = @Exec_Id
	) [sn2]
		on
			[sn].[COD_ABI] = [sn2].[COD_ABI]
			AND [sn].[COD_COMPAGNIA] = [sn2].[COD_COMPAGNIA]
			AND [sn].[COD_PRODOTTO_INTERNO] = [sn2].[COD_PRODOTTO_INTERNO]
			AND [sn].[COD_PRODOTTO] = [sn2].[COD_PRODOTTO]
			AND [sn].[Row_Id] = [sn2].[Row_Id]
			AND [sn].[Exec_Id] = [sn2].[Exec_Id]
	[sn].[COD_ABI] = [sn2].[COD_ABI]
	AND [sn].[COD_COMPAGNIA] = [sn2].[COD_COMPAGNIA]
	AND [sn].[COD_PRODOTTO_INTERNO] = [sn2].[COD_PRODOTTO_INTERNO]
	AND [sn].[COD_PRODOTTO] = [sn2].[COD_PRODOTTO]
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
	INSERT INTO [L0_SCARTI].[T_Vita_Polizze_Prodotto](
		[COD_ABI]
		,[COD_COMPAGNIA]
		,[COD_NATURA]
		,[COD_RAMO_ASSICURATIVO]
		,[COD_TARIFFA_ENTE]
		,[COD_PRODOTTO_INTERNO]
		,[COD_PRODOTTO]
		,[DES_PRODOTTO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[COD_ABI]
		,[COD_COMPAGNIA]
		,[COD_NATURA]
		,[COD_RAMO_ASSICURATIVO]
		,[COD_TARIFFA_ENTE]
		,[COD_PRODOTTO_INTERNO]
		,[COD_PRODOTTO]
		,[DES_PRODOTTO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Polizze_Prodotto]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Polizze_Prodotto] AS dst
	USING 
		( SELECT
			[COD_ABI]
			,[COD_COMPAGNIA]
			,[COD_NATURA]
			,[COD_RAMO_ASSICURATIVO]
			,[COD_TARIFFA_ENTE]
			,[COD_PRODOTTO_INTERNO]
			,[COD_PRODOTTO]
			,[DES_PRODOTTO]
			,[Row_Id]
		FROM [L0].[T_Vita_Polizze_Prodotto]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			on [src].[COD_ABI] = [dst].[COD_ABI]
			AND [src].[COD_COMPAGNIA] = [dst].[COD_COMPAGNIA]
			AND [src].[COD_PRODOTTO_INTERNO] = [dst].[COD_PRODOTTO_INTERNO]
			AND [src].[COD_PRODOTTO] = [dst].[COD_PRODOTTO]
	WHEN not matched THEN INSERT (
			[COD_ABI]
			,[COD_COMPAGNIA]
			,[COD_NATURA]
			,[COD_RAMO_ASSICURATIVO]
			,[COD_TARIFFA_ENTE]
			,[COD_PRODOTTO_INTERNO]
			,[COD_PRODOTTO]
			,[DES_PRODOTTO]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[COD_ABI]
			,[src].[COD_COMPAGNIA]
			,[src].[COD_NATURA]
			,[src].[COD_RAMO_ASSICURATIVO]
			,[src].[COD_TARIFFA_ENTE]
			,[src].[COD_PRODOTTO_INTERNO]
			,[src].[COD_PRODOTTO]
			,[src].[DES_PRODOTTO]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[COD_NATURA] = [src].[COD_NATURA]
			,[COD_RAMO_ASSICURATIVO] = [src].[COD_RAMO_ASSICURATIVO]
			,[COD_TARIFFA_ENTE] = [src].[COD_TARIFFA_ENTE]
			,[DES_PRODOTTO] = [src].[DES_PRODOTTO]
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