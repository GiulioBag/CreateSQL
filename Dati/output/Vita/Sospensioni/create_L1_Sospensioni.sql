CREATE TABLE [L1].[T_Vita_Sospensioni]( 
		[COD_ABI] [varchar] (5) NOT NULL, 
		[COD_CONTRATTO] [varchar] (34) NOT NULL, 
		[COD_RICHIEDENTE] [varchar] (1) NULL, 
		[DES_RICHIEDENTE] [varchar] (100) NULL, 
		[DAT_INIZIO_SOSPENSIONE] [date] NULL, 
		[DAT_FINE_SOSPENSIONE] [date] NULL, 
		[COD_OPERAZIONE] [varchar] (5) NOT NULL, 
		[DES_OPERAZIONE] [varchar] (100) NULL, 
		[DAT_OPERAZIONE] [date] NOT NULL, 
		[COD_ESITO_ADEGUATEZZA] [varchar] (2) NULL, 
		[DES_ESITO_ADEGUATEZZA] [varchar] (100) NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 CONSTRAINT [PK_L1_T_Sospensioni] PRIMARY KEY CLUSTERED 
(		[COD_ABI] ASC,
		[COD_CONTRATTO] ASC,
		[COD_OPERAZIONE] ASC,
		[DAT_OPERAZIONE] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] 