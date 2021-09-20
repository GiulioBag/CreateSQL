/* 
 ============================================= 

Autore: Giulio Bagnoli
Descrizione:
	Procedura di caricamento dalla tabella [L0].[T_Vita_Riscatti] alla tabella [L1].[T_Vita_Riscatti].
	Il caricamento segue una logica di MERGE (Insert + Update)
History:
	20/09/2021: Data di creazione
Esempio:
	exec [L1].[usp_Load_T_Vita_Riscatti]
		@Exec_ID = -2147483541

============================================= 
*/

CREATE	PROCEDURE [L1].[usp_Load_T_Vita_Riscatti]
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
		
		UPDATE [L0].[T_Vita_Riscatti]
		SET [BitMask_Scarti] = 0 
		; 
 

--		SET @Step = '2.1 Scarti: Applicazione criterio di scarto DUPLICATE_KEY'
--		;
--		UPDATE [sn]
--		SET [BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
--		FROM [L0].[T_Vita_Riscatti] [sn]
--		JOIN ( SELECT 
--			[sn].[COD_ABI]
--			,[sn].[COD_CONTRATTO]
--		FROM [L0].[T_Vita_Riscatti] [sn]
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
	FROM [L0].[T_Vita_Riscatti] [sn]
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
	FROM [L0].[T_Vita_Riscatti] [sn]
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
		FROM [L0].[T_Vita_Riscatti] [r]
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
 

	--Esclusione Date Null
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto EMPTY_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Riscatti] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[DAT_ORDINE] is null OR [sn].[DAT_ORDINE] = '')
			--OR ([sn].[DAT_RICHIESTA] is null OR [sn].[DAT_RICHIESTA] = '')
			--OR ([sn].[DAT_EFFETTO] is null OR [sn].[DAT_EFFETTO] = '')
			--OR ([sn].[DAT_EFFETTIVA] is null OR [sn].[DAT_EFFETTIVA] = '')
			--OR ([sn].[DAT_VALUTA_ACCREDITO] is null OR [sn].[DAT_VALUTA_ACCREDITO] = '')
			--OR ([sn].[DAT_INSERIMENTO_ORDINE] is null OR [sn].[DAT_INSERIMENTO_ORDINE] = '')
			--OR ([sn].[DAT_INSERIMENTO_MOVIMENTO] is null OR [sn].[DAT_INSERIMENTO_MOVIMENTO] = '')
		)
	; 
 

	--Esclusione date non Valide
	SET @Step = '2.4 Scarti: Applicazione criterio di scarto INVALID_DATE'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Riscatti] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_DATE' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[DAT_ORDINE] is not null AND [sn].[DAT_ORDINE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_ORDINE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_RICHIESTA] is not null AND [sn].[DAT_RICHIESTA]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_RICHIESTA], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_EFFETTO] is not null AND [sn].[DAT_EFFETTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_EFFETTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_EFFETTIVA] is not null AND [sn].[DAT_EFFETTIVA]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_EFFETTIVA], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_VALUTA_ACCREDITO] is not null AND [sn].[DAT_VALUTA_ACCREDITO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_VALUTA_ACCREDITO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_INSERIMENTO_ORDINE] is not null AND [sn].[DAT_INSERIMENTO_ORDINE]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_INSERIMENTO_ORDINE], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
			OR ([sn].[DAT_INSERIMENTO_MOVIMENTO] is not null AND [sn].[DAT_INSERIMENTO_MOVIMENTO]  <> '' AND  TRY_CONVERT (date, stuff(stuff([sn].[DAT_INSERIMENTO_MOVIMENTO], 6, 0, ' '), 3, 0, ' '), 106) IS NULL)
		)
	; 
 

	SET @Step = '2.5 Scarti: Applicazione criterio di scarto EMPTY_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Riscatti] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'EMPTY_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			--([sn].[PRO_VERSAMENTO] is null )
			--OR ([sn].[IMP_PAGAMENTO] is null )
			--OR ([sn].[IMP_SPESE] is null )
			--OR ([sn].[IMP_NETTO_OPERAZIONE] is null )
			--OR ([sn].[IMP_TASSAZIONE] is null )
			--OR ([sn].[IMP_VOLATILITA_PTF_MODIFICATO] is null )
			--OR ([sn].[IMP_VOLATILITA_PTF_REALE] is null )
			--OR ([sn].[IMP_VOLATILITA_PTF_IDEALE] is null )
			--OR ([sn].[NUM_SOGLIA] is null )
			--OR ([sn].[IMP_SOGLIA] is null )
			--OR ([sn].[PRC_SOGLIA_CONCTRZ_RAGGIUNTA] is null )
			--OR ([sn].[PRC_SOGLIA_CONCENTRAZIONE] is null )
		)
	; 
 

	--Esclusione numeric non Validi
	SET @Step = '2.6 Scarti: Applicazione criterio di scarto INVALID_NUMERIC'
	;
	UPDATE [sn]
	SET [sn].[BitMask_Scarti] = [sn].[BitMask_Scarti] + [scarti].[BitMask_Scarti]
	FROM [L0].[T_Vita_Riscatti] [sn]
	CROSS JOIN [L0_SCARTI].[T_Desc_Scarti] [scarti]
	where [Exec_Id] = @Exec_Id
		AND [scarti].[Cod_Scarto] = 'INVALID_NUMERIC' --Codice d'errore
		AND [scarti].[ID_Flusso] = @ID_Flusso
		AND [scarti].[Flag_Enabled] = 1
		AND (
			--Inserire qui in OR tutte le date su cui si vuole effettuare la validazione
			([sn].[PRO_VERSAMENTO] is NOT null AND TRY_CAST([PRO_VERSAMENTO] as int) IS  NULL))
			OR ([sn].[IMP_PAGAMENTO] is NOT null AND TRY_CAST(REPLACE([IMP_PAGAMENTO], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[IMP_SPESE] is NOT null AND TRY_CAST(REPLACE([IMP_SPESE], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[IMP_NETTO_OPERAZIONE] is NOT null AND TRY_CAST(REPLACE([IMP_NETTO_OPERAZIONE], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[IMP_TASSAZIONE] is NOT null AND TRY_CAST(REPLACE([IMP_TASSAZIONE], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[IMP_VOLATILITA_PTF_MODIFICATO] is NOT null AND TRY_CAST(REPLACE([IMP_VOLATILITA_PTF_MODIFICATO], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[IMP_VOLATILITA_PTF_REALE] is NOT null AND TRY_CAST(REPLACE([IMP_VOLATILITA_PTF_REALE], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[IMP_VOLATILITA_PTF_IDEALE] is NOT null AND TRY_CAST(REPLACE([IMP_VOLATILITA_PTF_IDEALE], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[NUM_SOGLIA] is NOT null AND TRY_CAST([NUM_SOGLIA] as int) IS  NULL))
			OR ([sn].[IMP_SOGLIA] is NOT null AND TRY_CAST(REPLACE([IMP_SOGLIA], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[PRC_SOGLIA_CONCTRZ_RAGGIUNTA] is NOT null AND TRY_CAST(REPLACE([PRC_SOGLIA_CONCTRZ_RAGGIUNTA], ',', '.') as NUMERIC (22,2)) IS  NULL))
			OR ([sn].[PRC_SOGLIA_CONCENTRAZIONE] is NOT null AND TRY_CAST(REPLACE([PRC_SOGLIA_CONCENTRAZIONE], ',', '.') as NUMERIC (22,2)) IS  NULL))
		)
	; 
 

	BEGIN TRANSACTION
	SET @Step = '3. Inserimento dati scartati su tabella dello schema [L0_SCARTI]'
	;
	INSERT INTO [L0_SCARTI].[T_Vita_Riscatti](
		[COD_ABI]
		,[COD_CONTRATTO]
		,[PRO_VERSAMENTO]
		,[COD_TIPO_LIQUIDIZIONE]
		,[DES_TIPO_LIQUIDAZIONE]
		,[COD_CAUSALE_OPERAZIONE]
		,[COD_STATO_OPERAZIONE]
		,[COD_FISCALE_BENEFICIARIO]
		,[COD_CANALE_EVENTO]
		,[COD_ABI_INSERIMENTO_MOVIMENTO]
		,[COD_UO_INSERIMENTO_MOVIMENTO]
		,[COD_OPERAZIONE]
		,[COD_ESITO_ADEGUATEZZA]
		,[COD_ADEGUATEZZA_ENTE]
		,[DAT_ORDINE]
		,[DAT_RICHIESTA]
		,[DAT_EFFETTO]
		,[DAT_EFFETTIVA]
		,[DAT_VALUTA_ACCREDITO]
		,[IMP_PAGAMENTO]
		,[IMP_SPESE]
		,[IMP_NETTO_OPERAZIONE]
		,[IMP_TASSAZIONE]
		,[IMP_VOLATILITA_PTF_MODIFICATO]
		,[IMP_VOLATILITA_PTF_REALE]
		,[IMP_VOLATILITA_PTF_IDEALE]
		,[NUM_SOGLIA]
		,[IMP_SOGLIA]
		,[PRC_SOGLIA_CONCTRZ_RAGGIUNTA]
		,[PRC_SOGLIA_CONCENTRAZIONE]
		,[DAT_INSERIMENTO_ORDINE]
		,[DAT_INSERIMENTO_MOVIMENTO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti])
	select
		[COD_ABI]
		,[COD_CONTRATTO]
		,[PRO_VERSAMENTO]
		,[COD_TIPO_LIQUIDIZIONE]
		,[DES_TIPO_LIQUIDAZIONE]
		,[COD_CAUSALE_OPERAZIONE]
		,[COD_STATO_OPERAZIONE]
		,[COD_FISCALE_BENEFICIARIO]
		,[COD_CANALE_EVENTO]
		,[COD_ABI_INSERIMENTO_MOVIMENTO]
		,[COD_UO_INSERIMENTO_MOVIMENTO]
		,[COD_OPERAZIONE]
		,[COD_ESITO_ADEGUATEZZA]
		,[COD_ADEGUATEZZA_ENTE]
		,[DAT_ORDINE]
		,[DAT_RICHIESTA]
		,[DAT_EFFETTO]
		,[DAT_EFFETTIVA]
		,[DAT_VALUTA_ACCREDITO]
		,[IMP_PAGAMENTO]
		,[IMP_SPESE]
		,[IMP_NETTO_OPERAZIONE]
		,[IMP_TASSAZIONE]
		,[IMP_VOLATILITA_PTF_MODIFICATO]
		,[IMP_VOLATILITA_PTF_REALE]
		,[IMP_VOLATILITA_PTF_IDEALE]
		,[NUM_SOGLIA]
		,[IMP_SOGLIA]
		,[PRC_SOGLIA_CONCTRZ_RAGGIUNTA]
		,[PRC_SOGLIA_CONCENTRAZIONE]
		,[DAT_INSERIMENTO_ORDINE]
		,[DAT_INSERIMENTO_MOVIMENTO]
		,[flag_altra_pol]
		,[Row_Id]
		,[Exec_Id]
		,[Bitmask_Scarti]
	FROM [L0].[T_Vita_Riscatti]
	WHERE [Exec_Id] = @Exec_Id
	AND  [BitMask_Scarti] <> 0
	; 
 

	SET @Step = '4. Esecuzione Merge'
	;
	MERGE [L1].[T_Vita_Riscatti] AS dst
	USING 
		( SELECT
			[COD_ABI]
			,[COD_CONTRATTO]
			,TRY_CAST([PRO_VERSAMENTO] as int) 
			,[COD_TIPO_LIQUIDIZIONE]
			,[DES_TIPO_LIQUIDAZIONE]
			,[COD_CAUSALE_OPERAZIONE]
			,[COD_STATO_OPERAZIONE]
			,[COD_FISCALE_BENEFICIARIO]
			,[COD_CANALE_EVENTO]
			,[COD_ABI_INSERIMENTO_MOVIMENTO]
			,[COD_UO_INSERIMENTO_MOVIMENTO]
			,[COD_OPERAZIONE]
			,[COD_ESITO_ADEGUATEZZA]
			,[COD_ADEGUATEZZA_ENTE]
			,TRY_CONVERT (date, stuff(stuff([DAT_ORDINE], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_RICHIESTA], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_EFFETTO], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_EFFETTIVA], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_VALUTA_ACCREDITO], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CAST(REPLACE([IMP_PAGAMENTO], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([IMP_SPESE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([IMP_NETTO_OPERAZIONE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([IMP_TASSAZIONE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([IMP_VOLATILITA_PTF_MODIFICATO], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([IMP_VOLATILITA_PTF_REALE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([IMP_VOLATILITA_PTF_IDEALE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST([NUM_SOGLIA] as int) 
			,TRY_CAST(REPLACE([IMP_SOGLIA], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([PRC_SOGLIA_CONCTRZ_RAGGIUNTA], ',', '.') as NUMERIC (22,2)) 
			,TRY_CAST(REPLACE([PRC_SOGLIA_CONCENTRAZIONE], ',', '.') as NUMERIC (22,2)) 
			,TRY_CONVERT (date, stuff(stuff([DAT_INSERIMENTO_ORDINE], 6, 0, ' '), 3, 0, ' '), 106)
			,TRY_CONVERT (date, stuff(stuff([DAT_INSERIMENTO_MOVIMENTO], 6, 0, ' '), 3, 0, ' '), 106)
			,[Row_Id]
		FROM [L0].[T_Vita_Riscatti]
		WHERE [Exec_Id] = @Exec_Id
		AND [BitMask_Scarti] = 0
		) AS src 
			on [src].[COD_ABI] = [dst].[COD_ABI]
			AND [src].[COD_CONTRATTO] = [dst].[COD_CONTRATTO]
	WHEN not matched THEN INSERT (
			[COD_ABI]
			,[COD_CONTRATTO]
			,[PRO_VERSAMENTO]
			,[COD_TIPO_LIQUIDIZIONE]
			,[DES_TIPO_LIQUIDAZIONE]
			,[COD_CAUSALE_OPERAZIONE]
			,[COD_STATO_OPERAZIONE]
			,[COD_FISCALE_BENEFICIARIO]
			,[COD_CANALE_EVENTO]
			,[COD_ABI_INSERIMENTO_MOVIMENTO]
			,[COD_UO_INSERIMENTO_MOVIMENTO]
			,[COD_OPERAZIONE]
			,[COD_ESITO_ADEGUATEZZA]
			,[COD_ADEGUATEZZA_ENTE]
			,[DAT_ORDINE]
			,[DAT_RICHIESTA]
			,[DAT_EFFETTO]
			,[DAT_EFFETTIVA]
			,[DAT_VALUTA_ACCREDITO]
			,[IMP_PAGAMENTO]
			,[IMP_SPESE]
			,[IMP_NETTO_OPERAZIONE]
			,[IMP_TASSAZIONE]
			,[IMP_VOLATILITA_PTF_MODIFICATO]
			,[IMP_VOLATILITA_PTF_REALE]
			,[IMP_VOLATILITA_PTF_IDEALE]
			,[NUM_SOGLIA]
			,[IMP_SOGLIA]
			,[PRC_SOGLIA_CONCTRZ_RAGGIUNTA]
			,[PRC_SOGLIA_CONCENTRAZIONE]
			,[DAT_INSERIMENTO_ORDINE]
			,[DAT_INSERIMENTO_MOVIMENTO]
			,[Exec_Id_InsertedOn]
			,[DateTime_InsertedOn]
			,[Row_Id_InsertedOn] 
		) VALUES (
			[src].[COD_ABI]
			,[src].[COD_CONTRATTO]
			,[src].[PRO_VERSAMENTO]
			,[src].[COD_TIPO_LIQUIDIZIONE]
			,[src].[DES_TIPO_LIQUIDAZIONE]
			,[src].[COD_CAUSALE_OPERAZIONE]
			,[src].[COD_STATO_OPERAZIONE]
			,[src].[COD_FISCALE_BENEFICIARIO]
			,[src].[COD_CANALE_EVENTO]
			,[src].[COD_ABI_INSERIMENTO_MOVIMENTO]
			,[src].[COD_UO_INSERIMENTO_MOVIMENTO]
			,[src].[COD_OPERAZIONE]
			,[src].[COD_ESITO_ADEGUATEZZA]
			,[src].[COD_ADEGUATEZZA_ENTE]
			,[src].[DAT_ORDINE]
			,[src].[DAT_RICHIESTA]
			,[src].[DAT_EFFETTO]
			,[src].[DAT_EFFETTIVA]
			,[src].[DAT_VALUTA_ACCREDITO]
			,[src].[IMP_PAGAMENTO]
			,[src].[IMP_SPESE]
			,[src].[IMP_NETTO_OPERAZIONE]
			,[src].[IMP_TASSAZIONE]
			,[src].[IMP_VOLATILITA_PTF_MODIFICATO]
			,[src].[IMP_VOLATILITA_PTF_REALE]
			,[src].[IMP_VOLATILITA_PTF_IDEALE]
			,[src].[NUM_SOGLIA]
			,[src].[IMP_SOGLIA]
			,[src].[PRC_SOGLIA_CONCTRZ_RAGGIUNTA]
			,[src].[PRC_SOGLIA_CONCENTRAZIONE]
			,[src].[DAT_INSERIMENTO_ORDINE]
			,[src].[DAT_INSERIMENTO_MOVIMENTO]
			,@Exec_Id
			,@Now
			,[src].[Row_Id])
	WHEN matched THEN UPDATE SET
			[PRO_VERSAMENTO] = [src].[PRO_VERSAMENTO]
			,[COD_TIPO_LIQUIDIZIONE] = [src].[COD_TIPO_LIQUIDIZIONE]
			,[DES_TIPO_LIQUIDAZIONE] = [src].[DES_TIPO_LIQUIDAZIONE]
			,[COD_CAUSALE_OPERAZIONE] = [src].[COD_CAUSALE_OPERAZIONE]
			,[COD_STATO_OPERAZIONE] = [src].[COD_STATO_OPERAZIONE]
			,[COD_FISCALE_BENEFICIARIO] = [src].[COD_FISCALE_BENEFICIARIO]
			,[COD_CANALE_EVENTO] = [src].[COD_CANALE_EVENTO]
			,[COD_ABI_INSERIMENTO_MOVIMENTO] = [src].[COD_ABI_INSERIMENTO_MOVIMENTO]
			,[COD_UO_INSERIMENTO_MOVIMENTO] = [src].[COD_UO_INSERIMENTO_MOVIMENTO]
			,[COD_OPERAZIONE] = [src].[COD_OPERAZIONE]
			,[COD_ESITO_ADEGUATEZZA] = [src].[COD_ESITO_ADEGUATEZZA]
			,[COD_ADEGUATEZZA_ENTE] = [src].[COD_ADEGUATEZZA_ENTE]
			,[DAT_ORDINE] = [src].[DAT_ORDINE]
			,[DAT_RICHIESTA] = [src].[DAT_RICHIESTA]
			,[DAT_EFFETTO] = [src].[DAT_EFFETTO]
			,[DAT_EFFETTIVA] = [src].[DAT_EFFETTIVA]
			,[DAT_VALUTA_ACCREDITO] = [src].[DAT_VALUTA_ACCREDITO]
			,[IMP_PAGAMENTO] = [src].[IMP_PAGAMENTO]
			,[IMP_SPESE] = [src].[IMP_SPESE]
			,[IMP_NETTO_OPERAZIONE] = [src].[IMP_NETTO_OPERAZIONE]
			,[IMP_TASSAZIONE] = [src].[IMP_TASSAZIONE]
			,[IMP_VOLATILITA_PTF_MODIFICATO] = [src].[IMP_VOLATILITA_PTF_MODIFICATO]
			,[IMP_VOLATILITA_PTF_REALE] = [src].[IMP_VOLATILITA_PTF_REALE]
			,[IMP_VOLATILITA_PTF_IDEALE] = [src].[IMP_VOLATILITA_PTF_IDEALE]
			,[NUM_SOGLIA] = [src].[NUM_SOGLIA]
			,[IMP_SOGLIA] = [src].[IMP_SOGLIA]
			,[PRC_SOGLIA_CONCTRZ_RAGGIUNTA] = [src].[PRC_SOGLIA_CONCTRZ_RAGGIUNTA]
			,[PRC_SOGLIA_CONCENTRAZIONE] = [src].[PRC_SOGLIA_CONCENTRAZIONE]
			,[DAT_INSERIMENTO_ORDINE] = [src].[DAT_INSERIMENTO_ORDINE]
			,[DAT_INSERIMENTO_MOVIMENTO] = [src].[DAT_INSERIMENTO_MOVIMENTO]
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