SELECT ROW_NUMBER() OVER (ORDER BY b.plaqueta) AS 'tora',
b.plaqueta, CAST(b.comprimento AS VARCHAR) as 'comprimento',
b.circmedia, b.circmedia-a.descontocirc AS 'circmedia_fat', 
CAST((FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento) AS VARCHAR) AS 'comprimento_fat',
CAST(CAST(((POWER((CAST((b.circmedia-a.descontocirc) AS FLOAT)/100),2))/(PI()*4))*(FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento) AS DECIMAL (5,3)) AS VARCHAR) AS 'volume',
CAST(CAST(POWER((CAST((b.circmedia-a.descontocirc) AS FLOAT)/100/4),2)*(FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento) AS DECIMAL (5,3)) AS VARCHAR) AS 'volume_hoppus',
f.descricao,
a.numero,
d.NOMEPROJETO,
c.CODTALHAO 
FROM EXP_LOTE a 
INNER JOIN EXP_TORA b ON a.id=b.id_lote AND a.deviceuuid=b.deviceuuid 
INNER JOIN SGIFLOR.dbo.CADTALHAO c ON b.CHATALHAO=c.CHATALHAO 
INNER JOIN SGIFLOR.dbo.CADPROJETO d ON c.CODPROJETO=d.CODPROJETO 
INNER JOIN EXP_INVENTARIO_LOTE e on e.id_lote=a.id and e.id_lote=b.id_lote and e.deviceuuid_lote=a.deviceuuid and e.deviceuuid_lote=b.deviceuuid
INNER JOIN EXP_INVENTARIO f on f.id=e.id_inv
where b.plaqueta=033818