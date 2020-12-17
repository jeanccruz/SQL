SELECT
(
SELECT TOP 1 d.CODTALHAO FROM
CADTALHAO d INNER JOIN CADPROJETO e ON d.CODPROJETO=e.CODPROJETO
INNER JOIN EXP_INVENTARIO_LOTE f ON a.deviceuuid=f.deviceuuid_lote AND a.id=f.id_lote
INNER JOIN EXP_INVENTARIO g ON g.id=f.id_inv WHERE b.CHATALHAO=d.CHATALHAO GROUP BY d.CODTALHAO, e.NOMEPROJETO, g.dt_add, g.descricao ORDER BY COUNT(b.id) DESC
) AS 'Talhão',
(
SELECT TOP 1 e.NOMEPROJETO FROM
CADTALHAO d INNER JOIN CADPROJETO e ON d.CODPROJETO=e.CODPROJETO
INNER JOIN EXP_INVENTARIO_LOTE f ON a.deviceuuid=f.deviceuuid_lote AND a.id=f.id_lote
INNER JOIN EXP_INVENTARIO g ON g.id=f.id_inv WHERE b.CHATALHAO=d.CHATALHAO GROUP BY d.CODTALHAO, e.NOMEPROJETO, g.dt_add, g.descricao ORDER BY COUNT(b.id) DESC
) AS 'Projeto',
(
SELECT TOP 1 CONVERT(CHAR(10),g.dt_add, 111) FROM
CADTALHAO d INNER JOIN CADPROJETO e ON d.CODPROJETO=e.CODPROJETO
INNER JOIN EXP_INVENTARIO_LOTE f ON a.deviceuuid=f.deviceuuid_lote AND a.id=f.id_lote
INNER JOIN EXP_INVENTARIO g ON g.id=f.id_inv WHERE b.CHATALHAO=d.CHATALHAO GROUP BY d.CODTALHAO, e.NOMEPROJETO, g.dt_add, g.descricao ORDER BY COUNT(b.id) DESC
) AS 'Data inventário',
(
SELECT TOP 1 g.descricao FROM
CADTALHAO d INNER JOIN CADPROJETO e ON d.CODPROJETO=e.CODPROJETO
INNER JOIN EXP_INVENTARIO_LOTE f ON a.deviceuuid=f.deviceuuid_lote AND a.id=f.id_lote
INNER JOIN EXP_INVENTARIO g ON g.id=f.id_inv WHERE b.CHATALHAO=d.CHATALHAO GROUP BY d.CODTALHAO, e.NOMEPROJETO, g.dt_add, g.descricao ORDER BY COUNT(b.id) DESC
) AS 'Descrição',
a.numero,
c.descricao,
CONVERT(CHAR(10),MAX(b.dt_add), 111) AS 'data lote',
a.descontocirc, a.stepcomprimento, COUNT(b.id) AS 'toras',
SUM(ROUND(((POWER((CAST((b.circmedia-a.descontocirc) AS FLOAT)/100),2))/(PI()*4))*(FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento),3)) AS 'volume',
SUM(ROUND(POWER((CAST((b.circmedia-a.descontocirc) AS FLOAT)/100/4),2)*(FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento),3)) AS 'volume_hoppus',
ROUND(AVG(CAST(b.circmedia-a.descontocirc AS FLOAT)),2) AS 'avg_circmedia_fat',
MAX(b.circmedia-a.descontocirc) AS 'max_circmedia_fat', MIN(b.circmedia-a.descontocirc) AS 'min_circmedia_fat',
ROUND(AVG((FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento)),2) AS 'avg_comprimento_fat',
MAX((FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento)) AS 'max_comprimento_fat',
MIN((FLOOR(b.comprimento/a.stepcomprimento)*a.stepcomprimento)) AS 'min_comprimento_fat' FROM
EXP_LOTE a
INNER JOIN EXP_TORA b ON a.id=b.id_lote AND a.deviceuuid=b.deviceuuid
INNER JOIN EXP_PRODUTO c ON a.id_produto=c.id
GROUP BY a.deviceuuid, a.id, b.CHATALHAO, a.numero, a.descontocirc, a.stepcomprimento, c.descricao;