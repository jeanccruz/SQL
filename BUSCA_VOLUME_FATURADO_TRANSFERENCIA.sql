select									
	m.DATASAIDA								
	,MONTH(m.DATAMOVIMENTO) as MES			
	,m.IDMOV
	,M.CODTMV					
	,m.NUMEROMOV								
	,null								
	,F.NOMEFANTASIA								
	,CASE WHEN P.DESCRICAO = 'LENHA' THEN 'LENHA' ELSE SUBSTRING(P.DESCRICAO,1,CHARINDEX(' ',P.DESCRICAO)) END AS GRUPO								
	,p.DESCRICAO								
	,i.CODUND								
	,i.QUANTIDADE								
	,NULL								
	,NULL								
	,m.CODFILIAL								
	,tm.NOME								
	,i.VALORBRUTOITEM/i.QUANTIDADE 	PREÇO	 						
	,i.VALORBRUTOITEM								
	,rtrim(ltrim(SUBSTRING(H.HISTORICOLONGO,CHARINDEX('T:',cast(h.historicolongo as varchar(max)),1) -5 ,4)	))							
	,replace(REPLACE(SUBSTRING(H.HISTORICOLONGO,CHARINDEX('T:',cast(h.historicolongo as varchar(max)),1) + 2 ,5)	,'.','') , ' ','')							
	,M.SEGUNDONUMERO								
FROM									
TMOV M									
join TMOVHISTORICO h on h.CODCOLIGADA = m.CODCOLIGADA and h.IDMOV = m.IDMOV									
LEFT JOIN FCFO F ON F.CODCFO = M.CODCFO AND F.CODCOLIGADA = M.CODCOLCFO									
JOIN TITMMOV I ON I.IDMOV = M.IDMOV AND I.CODCOLIGADA = M.CODCOLIGADA									
JOIN TPRD P ON P.IDPRD = I.IDPRD AND M.CODCOLIGADA = P.CODCOLIGADA									
JOIN TTMV TM ON TM.CODTMV = M.CODTMV AND TM.CODCOLIGADA = M.CODCOLIGADA									
where 									
M.CODCOLIGADA = 1 									
AND M.CODFILIAL <> 18			AND M.CODFILIAL <> 19						
			
and M.CODTMV IN ('3.1.01','3.1.05')									
AND M.STATUS <> 'C'									
--AND SEGUNDONUMERO <> '0'									
AND YEAR(M.DATASAIDA) = 2017								
ORDER BY 1,3