USE [CPSilvic];
DROP VIEW [dbo].[view_ExpLote];
CREATE VIEW [dbo].[view_ExpLote] AS
SELECT
 MAX(b.dt_add) AS DataLote,
 YEAR(MAX(b.dt_add)) AS AnoLote,
 MONTH(MAX(b.dt_add)) AS MêsLote,
 a.numero AS Lote,
 ROUND(AVG(CAST(b.circmedia-a.descontocirc AS FLOAT)),2) AS CircMédia,
 ROUND(AVG(CAST((FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento) AS FLOAT)),2) AS Comprimento,
 ROUND(SUM(ROUND(((POWER((CAST((b.circmedia-a.descontocirc) AS FLOAT)/100),2))/(PI()*4))*(FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento),3)),3) AS volume,
 YEAR(g.dt_add) AS AnoInv,
 MONTH(g.dt_add) AS MesInv,
 g.descricao AS Inventário,
 g.numero,
 d.CODPROJETO AS Projeto,
 d.CODTALHAO AS Talhão,
 k.SIGLA,
 k.ESTADO,
 c.descricao AS Produto
FROM EXP_LOTE a
INNER JOIN (SELECT * FROM (SELECT deviceuuid, id_lote, chatalhao, conta, ROW_NUMBER() OVER(PARTITION BY deviceuuid, id_lote ORDER BY deviceuuid, id_lote, conta DESC, chatalhao) AS pos FROM (SELECT chatalhao, deviceuuid, id_lote, COUNT(1) AS conta FROM EXP_TORA GROUP BY deviceuuid, id_lote, chatalhao) x) y
WHERE pos = 1) tmp ON a.deviceuuid=tmp.deviceuuid AND a.id=tmp.id_lote
INNER JOIN EXP_TORA b ON a.id=b.id_lote AND a.deviceuuid=b.deviceuuid
INNER JOIN EXP_PRODUTO c ON a.id_produto=c.id
INNER JOIN [SGIFLOR]..CADTALHAO d ON tmp.chatalhao=d.CHATALHAO
INNER JOIN [SGIFLOR]..CADPROJETO e ON d.CODPROJETO=e.CODPROJETO
LEFT JOIN EXP_INVENTARIO_LOTE f ON a.deviceuuid=f.deviceuuid_lote AND a.id=f.id_lote
LEFT JOIN EXP_INVENTARIO g ON f.id_inv=g.id
LEFT JOIN (SELECT deviceuuid, id_lote, COUNT(1) AS 'fotos' FROM EXP_LOTEIMG GROUP BY deviceuuid, id_lote) h ON a.deviceuuid=h.deviceuuid AND a.id=h.id_lote
LEFT JOIN DISPOSITIVOS i ON a.deviceuuid=i.deviceuuid
INNER JOIN [SGIFLOR]..CADREGIAO j ON e.CODREGIAO=j.CODREGIAO
INNER JOIN [SGIFLOR]..CADGRPREGIAO k ON j.CODGRPREGIAO=k.CODGRPREGIAO
GROUP BY a.deviceuuid, a.id, a.numero, g.dt_add, g.descricao, g.numero, d.CODPROJETO, d.CODTALHAO, k.SIGLA, k.ESTADO, c.descricao;