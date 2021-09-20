CREATE TABLE [L1].[T_Vita_Anagrafica_Clienti]( 
		[NUM_ANNO_MESE] [int] NULL, 
		[COD_SUPERNSG] [varchar] (16) NULL, 
		[COD_ABI] [varchar] (5) NOT NULL, 
		[COD_NDG] [varchar] (13) NOT NULL, 
		[COD_FISCALE] [varchar] (16) NULL, 
		[COD_PARTITA_IVA] [varchar] (11) NULL, 
		[COD_FORMA_GIURIDICA] [varchar] (5) NULL, 
		[DAT_NASCITA] [date] NULL, 
		[COD_PROFILO_INVESTITORE] [varchar] (20) NULL, 
		[DES_PROFILO_INVESTITORE] [varchar] (100) NULL, 
		[COD_ESPERIENZA_INVESTIMENTO] [varchar] (20) NULL, 
		[DES_ESPERIENZA_INVESTIMENTO] [varchar] (100) NULL, 
		[COD_ABI_FIL_PTF] [varchar] (5) NULL, 
		[COD_FILIALE_PORTAFOGLIO] [varchar] (5) NULL, 
		[COD_PORTAFOGLIO] [varchar] (2) NULL, 
		[COD_TIPO_PORTAFOGLIO] [varchar] (1) NULL, 
		[COD_FILIALE_RIFERIMENTO] [varchar] (5) NULL, 
		[FLAG_FIX_PROF_INVST] [bit] NULL, 
		[FLAG_FIX_ESPR_INVST] [bit] NULL, 
		[Exec_Id_InsertedOn] [int] NULL,
		[DateTime_InsertedOn] [datetime] NULL,
		[Row_Id_InsertedOn] [int] NULL,
		[Exec_Id_UpdatedOn] [int] NULL,
		[DateTime_UpdatedOn] [datetime] NULL,
		[Row_Id_UpdatedOn] [int] NULL
 CONSTRAINT [PK_L1_T_Anagrafica_Clienti] PRIMARY KEY CLUSTERED 
(		[COD_ABI] ASC,
		[COD_NDG] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] 