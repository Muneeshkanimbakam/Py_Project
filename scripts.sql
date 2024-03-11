SELECT /*+ parallel (a,6) */ COUNT(b.ban), d.adr_state_code
FROM billing_account b
JOIN address_name_link a ON b.ban = a.ban
JOIN address_data d ON a.address_id = d.address_id
WHERE b.company_owner_id = '4'
    AND b.ban_status NOT IN ('N', 'C', 'T')
    AND a.expiration_date IS NULL
    AND a.link_type = 'B'
    AND d.adr_state_code IN ('GA', 'AL', 'MI')
GROUP BY d.adr_state_code;
