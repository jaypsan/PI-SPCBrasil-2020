import pandas as pd

low_memory = False

df_PSF = pd.read_csv(r'C:\Users\John-PC\Desktop\PI-SPCBrasil-2020\database\PES_FIS_Novo.csv', delimiter=',', encoding='iso-8859-1')
df_END_PSF = pd.read_csv(r'C:\Users\John-PC\Desktop\PI-SPCBrasil-2020\database\END_PES_FIS_Novo.csv', delimiter=',', encoding='iso-8859-1')
df_OPR = pd.read_csv(r'C:\Users\John-PC\Desktop\PI-SPCBrasil-2020\database\OPR_Novo.csv', delimiter=',', encoding='iso-8859-1')

df_PSF = df_PSF.rename(columns={'id': 'ID', 'cpf': 'CPF'})
df_END_PSF = df_END_PSF.rename(columns={'id_pessoa_fisica': 'ID'})
df_OPR = df_OPR.rename(columns={'doc_cli': 'CPF'})

df_OPRuPSF = pd.merge(df_OPR, df_PSF, on='CPF')
del df_OPR, df_PSF

df_OPRuPSFuEndPsf = pd.merge(df_OPRuPSF, df_END_PSF, on='ID')
del df_OPRuPSF, df_END_PSF

df_OPRuPSFuEndPsf['vlr_ctrd_fta_tfm'] = df_OPRuPSFuEndPsf['vlr_ctrd_fta_tfm'].astype('float64')

df_Total_B05_REM = df_OPRuPSFuEndPsf.query('(cod_mdl == "B05")')
del df_OPRuPSFuEndPsf

df_Total_B05_REM = df_Total_B05_REM.drop(columns=['id_opr_cad_pos','CPF','tip_cli','qtd_pcl','dat_vct_ult_pcl','sdo_ddr_tfm','vlr_ctrd', 'id_ult_rss_opr', 'id_mdl', 'cod_mdl', 'id_fnt', 'ID', 'idc_sexo', 'ano_dat_nascimento', 'nom_cidade'])

df_NORTE = df_Total_B05_REM.query('(des_estado == "ACRE") or (des_estado == "AMAPA") or (des_estado == "AMAZONAS") or (des_estado == "PARA") or (des_estado == "RONDONIA") or (des_estado == "RORAIMA") or (des_estado == "TOCANTINS")').drop(columns='des_estado')
df_ACRE = df_Total_B05_REM.query('(des_estado == "ACRE")').drop(columns='des_estado')
df_AMAPA = df_Total_B05_REM.query('(des_estado == "AMAPA")').drop(columns='des_estado')
df_AMAZONAS = df_Total_B05_REM.query('(des_estado == "AMAZONAS")').drop(columns='des_estado')
df_PARA = df_Total_B05_REM.query('(des_estado == "PARA")').drop(columns='des_estado')
df_RONDONIA = df_Total_B05_REM.query('(des_estado == "RONDONIA")').drop(columns='des_estado')
df_RORAIMA = df_Total_B05_REM.query('(des_estado == "RORAIMA")').drop(columns='des_estado')
df_TOCANTINS = df_Total_B05_REM.query('(des_estado == "TOCANTINS")').drop(columns='des_estado')

df_NORDESTE = df_Total_B05_REM.query('(des_estado == "ALAGOAS") or (des_estado == "BAHIA") or (des_estado == "CEARA") or (des_estado == "MARANHAO") or (des_estado == "PARAIBA") or (des_estado == "PERNAMBUCO") or (des_estado == "PIAUI") or (des_estado == "RIO GRANDE DO NORTE") or (des_estado == "SERGIPE")').drop(columns='des_estado')
df_ALAGOAS = df_Total_B05_REM.query('(des_estado == "ALAGOAS")').drop(columns='des_estado')
df_BAHIA = df_Total_B05_REM.query('(des_estado == "BAHIA")').drop(columns='des_estado')
df_CEARA = df_Total_B05_REM.query('(des_estado == "CEARA")').drop(columns='des_estado')
df_MARANHAO = df_Total_B05_REM.query('(des_estado == "MARANHAO")').drop(columns='des_estado')
df_PARAIBA = df_Total_B05_REM.query('(des_estado == "PARAIBA")').drop(columns='des_estado')
df_PERNAMBUCO = df_Total_B05_REM.query('(des_estado == "PERNAMBUCO")').drop(columns='des_estado')
df_PIAUI = df_Total_B05_REM.query('(des_estado == "PIAUI")').drop(columns='des_estado')
df_RIO_GRANDE_DO_NORTE = df_Total_B05_REM.query('(des_estado == "RIO GRANDE DO NORTE")').drop(columns='des_estado')
df_SERGIPE = df_Total_B05_REM.query('(des_estado == "SERGIPE")').drop(columns='des_estado')

df_CENTRO_OESTE = df_Total_B05_REM.query('(des_estado == "DISTRITO FEDERAL") or (des_estado == "GOIAS") or (des_estado == "MATO GROSSO") or (des_estado == "MATO GROSSO DO SUL")').drop(columns='des_estado')
df_DISTRITO_FEDERAL = df_Total_B05_REM.query('(des_estado == "DISTRITO FEDERAL")').drop(columns='des_estado')
df_GOIAS = df_Total_B05_REM.query('(des_estado == "GOIAS")').drop(columns='des_estado')
df_MATO_GROSSO = df_Total_B05_REM.query('(des_estado == "MATO GROSSO")').drop(columns='des_estado')
df_MATO_GROSSO_DO_SUL = df_Total_B05_REM.query('(des_estado == "MATO GROSSO DO SUL")').drop(columns='des_estado')

df_SUDESTE = df_Total_B05_REM.query('(des_estado == "ESPIRITO SANTO") or (des_estado == "MINAS GERAIS") or (des_estado == "RIO DE JANEIRO") or (des_estado == "SAO PAULO")').drop(columns='des_estado')
df_ESPIRITO_SANTO = df_Total_B05_REM.query('(des_estado == "ESPIRITO SANTO")').drop(columns='des_estado')
df_MINAS_GERAIS = df_Total_B05_REM.query('(des_estado == "MINAS GERAIS")').drop(columns='des_estado')
df_RIO_DE_JANEIRO = df_Total_B05_REM.query('(des_estado == "RIO DE JANEIRO")').drop(columns='des_estado')
df_SAO_PAULO = df_Total_B05_REM.query('(des_estado == "SAO PAULO")').drop(columns='des_estado')

df_SUL = df_Total_B05_REM.query('(des_estado == "PARANA") or (des_estado == "RIO GRANDE DO SUL") or (des_estado == "SANTA CATARINA")').drop(columns='des_estado')
df_PARANA = df_Total_B05_REM.query('(des_estado == "PARANA")').drop(columns='des_estado')
df_RIO_GRANDE_DO_SUL = df_Total_B05_REM.query('(des_estado == "RIO GRANDE DO SUL")').drop(columns='des_estado')
df_SANTA_CATARINA = df_Total_B05_REM.query('(des_estado == "SANTA CATARINA")').drop(columns='des_estado')

df_BRASIL = df_Total_B05_REM.drop(columns='des_estado')
df_BRASIL = df_BRASIL.rename(columns={'vlr_ctrd_fta_tfm': 'BRASIL'})

df_NORTE = df_NORTE.join(df_ACRE, lsuffix='_norte', rsuffix='_acre')
df_NORTE = df_NORTE.join(df_AMAPA, lsuffix='_acre', rsuffix='_amapa')
df_NORTE = df_NORTE.join(df_AMAZONAS, lsuffix='_amapa', rsuffix='_amazonas')
df_NORTE = df_NORTE.join(df_PARA, lsuffix='_amazonas', rsuffix='_para') 
df_NORTE = df_NORTE.join(df_RONDONIA, lsuffix='_para', rsuffix='_rondonia')
df_NORTE = df_NORTE.join(df_RORAIMA, lsuffix='_rondonia', rsuffix='_roraima')
df_NORTE = df_NORTE.join(df_TOCANTINS, lsuffix='_roraima', rsuffix='_tocantins')
df_NORTE = df_NORTE.rename(columns={'vlr_ctrd_fta_tfm_norte': 'NORTE', 'vlr_ctrd_fta_tfm_acre': 'ACRE',
                                   'vlr_ctrd_fta_tfm_amapa': 'AMAPA', 'vlr_ctrd_fta_tfm_amazonas': 'AMAZONAS',
                                   'vlr_ctrd_fta_tfm_para': 'PARA', 'vlr_ctrd_fta_tfm_rondonia': 'RONDONIA',
                                   'vlr_ctrd_fta_tfm_roraima': 'RORAIMA', 'vlr_ctrd_fta_tfm_tocantins': 'TOCANTINS'})

df_NORDESTE = df_NORDESTE.join(df_ALAGOAS, lsuffix='_nodeste', rsuffix='_alagoas')
df_NORDESTE = df_NORDESTE.join(df_BAHIA, lsuffix='_alagoas', rsuffix='_bahia')
df_NORDESTE = df_NORDESTE.join(df_CEARA, lsuffix='_bahia', rsuffix='_ceara')
df_NORDESTE = df_NORDESTE.join(df_MARANHAO, lsuffix='_ceara', rsuffix='_maranhao')
df_NORDESTE = df_NORDESTE.join(df_PARAIBA, lsuffix='_maranhao', rsuffix='_paraiba')
df_NORDESTE = df_NORDESTE.join(df_PERNAMBUCO, lsuffix='_paraiba', rsuffix='_pernambuco')
df_NORDESTE = df_NORDESTE.join(df_PIAUI, lsuffix='_pernambuco', rsuffix='_piaui')
df_NORDESTE = df_NORDESTE.join(df_RIO_GRANDE_DO_NORTE, lsuffix='_piaui', rsuffix='_rio_grande_do_norte')
df_NORDESTE = df_NORDESTE.join(df_SERGIPE,lsuffix='_rio_grande_do_norte', rsuffix='_sergipe')
df_NORDESTE = df_NORDESTE.rename(columns={'vlr_ctrd_fta_tfm_nodeste': 'NORDESTE', 'vlr_ctrd_fta_tfm_alagoas': 'ALAGOAS',
                                   'vlr_ctrd_fta_tfm_bahia': 'BAHIA', 'vlr_ctrd_fta_tfm_ceara': 'CEARA',
                                   'vlr_ctrd_fta_tfm_maranhao': 'MARANHAO', 'vlr_ctrd_fta_tfm_paraiba': 'PARAIBA',
                                   'vlr_ctrd_fta_tfm_pernambuco': 'PERNAMBUCO', 'vlr_ctrd_fta_tfm_piaui': 'PIAUI',
                                   'vlr_ctrd_fta_tfm_rio_grande_do_norte': 'RIO GRANDE DO NORTE', 'vlr_ctrd_fta_tfm_sergipe': 'SERGIPE'})

df_CENTRO_OESTE = df_CENTRO_OESTE.join(df_DISTRITO_FEDERAL, lsuffix='_centro_oeste', rsuffix='_distrito_federal')
df_CENTRO_OESTE = df_CENTRO_OESTE.join(df_GOIAS, lsuffix='_distrito_federal', rsuffix='_goiais')
df_CENTRO_OESTE = df_CENTRO_OESTE.join(df_MATO_GROSSO, lsuffix='_goiais', rsuffix='_mato_grosso')
df_CENTRO_OESTE = df_CENTRO_OESTE.join(df_MATO_GROSSO_DO_SUL, lsuffix='_mato_grosso', rsuffix='_mato_grosso_do_sul')
df_CENTRO_OESTE = df_CENTRO_OESTE.rename(columns={'vlr_ctrd_fta_tfm_centro_oeste': 'CENTRO OESTE', 
                                                  'vlr_ctrd_fta_tfm_distrito_federal': 'DISTRITO FEDERAL',
                                                  'vlr_ctrd_fta_tfm_goiais': 'GOIAIS', 
                                                  'vlr_ctrd_fta_tfm_mato_grosso': 'MATO GROSSO',
                                                  'vlr_ctrd_fta_tfm': 'MATO GROSSO DO SUL'})

df_SUDESTE = df_SUDESTE.join(df_ESPIRITO_SANTO, lsuffix='_sudeste', rsuffix='_espirito_santo')
df_SUDESTE = df_SUDESTE.join(df_MINAS_GERAIS, lsuffix='_espirito_santo', rsuffix='_minas_gerais')
df_SUDESTE = df_SUDESTE.join(df_RIO_DE_JANEIRO, lsuffix='_minas_gerais', rsuffix='_rio_de_janeiro')
df_SUDESTE = df_SUDESTE.join(df_SAO_PAULO, lsuffix='_rio_de_janeiro', rsuffix='_sao_paulo')
df_SUDESTE = df_SUDESTE.rename(columns={'vlr_ctrd_fta_tfm_sudeste': 'SUDESTE', 
                                                  'vlr_ctrd_fta_tfm_espirito_santo': 'ESPIRITO SANTO',
                                                  'vlr_ctrd_fta_tfm_minas_gerais': 'MINAS GERAIS', 
                                                  'vlr_ctrd_fta_tfm_rio_de_janeiro': 'RIO DE JANEIRO',
                                                  'vlr_ctrd_fta_tfm': 'SAO PAULO'})

df_SUL = df_SUL.join(df_PARANA, lsuffix='_sul', rsuffix='_parana')
df_SUL = df_SUL.join(df_RIO_GRANDE_DO_SUL, lsuffix='_parana', rsuffix='_rio_grande_do_sul')
df_SUL = df_SUL.join(df_SANTA_CATARINA,lsuffix='_rio_grande_do_sul', rsuffix='_santa_catarina')
df_SUL = df_SUL.rename(columns={'vlr_ctrd_fta_tfm_sul': 'SUL', 
                                'vlr_ctrd_fta_tfm_parana': 'PARANA',
                                'vlr_ctrd_fta_tfm_rio_grande_do_sul': 'RIO GRANDE DO SUL', 
                                'vlr_ctrd_fta_tfm_santa_catarina': 'SANTA CATARINA'})

del df_Total_B05_REM, df_ACRE, df_AMAPA, df_AMAZONAS, df_PARA, df_RONDONIA, df_RORAIMA, df_TOCANTINS, df_ALAGOAS, df_BAHIA, df_CEARA, df_MARANHAO, df_PARAIBA, df_PERNAMBUCO, df_PIAUI, df_RIO_GRANDE_DO_NORTE, df_SERGIPE, df_DISTRITO_FEDERAL, df_MATO_GROSSO, df_GOIAS, df_MATO_GROSSO_DO_SUL, df_ESPIRITO_SANTO, df_MINAS_GERAIS, df_RIO_DE_JANEIRO, df_SAO_PAULO, df_PARANA, df_RIO_GRANDE_DO_SUL, df_SANTA_CATARINA
