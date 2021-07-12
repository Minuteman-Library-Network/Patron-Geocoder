SELECT DISTINCT a.id, a.street, a.city, a.region, a.zip 
FROM 
(
SELECT p.patron_record_id AS id,
COALESCE(p.addr1,'') AS street,
COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(p.city,'\d','','g'),'\sma$','','i'),'') AS city,
COALESCE(CASE WHEN p.region = '' AND (LOWER(p.city) ~ '\sma$' OR pr.pcode3 BETWEEN '1' AND '200') THEN 'MA' ELSE p.region END,'') AS region,
COALESCE(SUBSTRING(p.postal_code,'^\d{5}'),'') AS zip,
s.content,
rm.creation_date_gmt, 
pr.ptype_code 

FROM sierra_view.patron_record_address p 
JOIN sierra_view.record_metadata rm 
ON p.patron_record_id = rm.id AND LOWER(p.addr1) !~ '^p\.?\s?o' AND p.patron_record_address_type_id = '1' 
LEFT JOIN sierra_view.subfield s 
ON p.patron_record_id = s.record_id  AND s.field_type_code = 'k' AND s.tag = 'd' 
JOIN sierra_view.patron_record pr 
ON p.patron_record_id = pr.id 

WHERE (s.content IS NULL AND pr.ptype_code NOT IN ('43','199','204','205','206','207','254')) OR TO_DATE(s.content,'YYYY-MM-DD') < rm.record_last_updated_gmt::DATE
--AND p.addr1 != '' AND p.city != ''
--Gather 10000 patron records, ordered by records that lack a census field then the olded last updated date within that field
ORDER BY CASE WHEN s.content IS NULL THEN 1 ELSE 2 END, s.content 
LIMIT 10000) a
