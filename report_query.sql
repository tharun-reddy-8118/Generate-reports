
SELECT 'Kiosks Count' AS Name, COUNT(1)::BIGINT AS AIG,

    (SELECT COUNT(1)::BIGINT
     FROM device_k d
     WHERE d."status" = 'ACTIVE' AND d.device_name LIKE '%REG%') AS Kamineni,

    (SELECT COUNT(1)::BIGINT
     FROM device_a d
     WHERE d."status" = 'ACTIVE' AND d.device_name LIKE '%KIOSK%') AS Aster,

    (SELECT COUNT(1)::BIGINT
     FROM device_c d
     WHERE d."status" = 'ACTIVE' AND d.device_name LIKE '%KIOSK%') AS Continental,

    (SELECT COUNT(1)::BIGINT
     FROM device_kims d
     WHERE d."status" = 'ACTIVE') AS Kims

FROM device d
WHERE d."status" = 'ACTIVE' AND d.device_name LIKE '%REG%'

UNION ALL


SELECT 'Total Successful Registrations' AS Name, COUNT(u.id)::BIGINT AS AIG,

    (SELECT COUNT(u.id)::BIGINT
     FROM user_k u
     LEFT JOIN device_k d ON u.device_id = d.device_code
     WHERE u.registration_type = 'MOBILE_REGISTRATION'
     AND DATE(u.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Kamineni,

    (SELECT COUNT(u.id)::BIGINT
     FROM user_a u
     LEFT JOIN device_a d ON u.device_id = d.device_code
     WHERE u.registration_type != 'VITAL'
     AND DATE(u.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Aster,

    (SELECT COUNT(u.id)::BIGINT
     FROM user_c u
     LEFT JOIN device_c d ON u.device_id = d.device_code
     WHERE u.registration_type != 'VITAL'
     AND DATE(u.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Continental,

    (SELECT COUNT(DISTINCT u.patient_id)::BIGINT
     FROM appointment_details_kims u
     LEFT JOIN device_kims d ON u.device_id = d.device_code
     WHERE u.status = 'NEW_PATIENT'
     AND DATE(u.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Kims

FROM user u
LEFT JOIN device d ON u.device_id = d.device_code
WHERE u.registration_type != 'VITAL'
AND DATE(u.created_at) BETWEEN %(start_date)s AND %(end_date)s

UNION ALL


SELECT 'Avg. Transaction time' AS Name,
EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT AS AIG,

    (SELECT EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT
     FROM track_screen_k ts
     LEFT JOIN device_k d ON ts.device_id = d.device_code
     WHERE d.device_name LIKE '%REG%'
     AND ts."action" = 'REGISTRATION_SUCCESS'
     AND DATE(ts.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Kamineni,

    (SELECT EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT
     FROM track_screen_a ts
     LEFT JOIN device_a d ON ts.device_id = d.device_code
     WHERE d.device_name LIKE '%KIOSK%'
     AND ts."action" = 'REGISTRATION_SUCCESS'
     AND DATE(ts.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Aster,

    (SELECT EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT
     FROM track_screen_c ts
     LEFT JOIN device_c d ON ts.device_id = d.device_code
     WHERE d.device_name LIKE '%KIOSK%'
     AND ts."action" = 'REGISTRATION_SUCCESS'
     AND DATE(ts.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Continental,

    (SELECT EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT
     FROM track_screen_kims ts
     LEFT JOIN device_kims d ON ts.device_id = d.device_code
     WHERE ts."action" = 'REGISTRATION_SUCCESS'
     AND DATE(ts.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Kims

FROM track_screen ts
LEFT JOIN device d ON ts.device_id = d.device_code
WHERE d.device_name LIKE '%REG%'
AND ts."action" = 'REGISTRATION_SUCCESS'
AND DATE(ts.created_at) BETWEEN %(start_date)s AND %(end_date)s

UNION ALL


SELECT 'Avg. Rating' AS Name,
ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1) AS AIG,

    (SELECT ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1)
     FROM user_feedback_k uf
     LEFT JOIN fb_category_k fb ON uf.category_id = fb.id
     WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
     AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Kamineni,

    (SELECT ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1)
     FROM user_feedback_a uf
     LEFT JOIN fb_category_a fb ON uf.category_id = fb.id
     WHERE fb."code" LIKE '%REGISTRATION_PROCESS%' OR fb."code" LIKE '%REPORT_PRINT%'
     AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Aster,

    (SELECT ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1)
     FROM user_feedback_c uf
     LEFT JOIN fb_category_c fb ON uf.category_id = fb.id
     WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
     AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Continental,

    (SELECT ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1)
     FROM user_feedback_kims uf
     LEFT JOIN fb_category_kims fb ON uf.category_id = fb.id
     WHERE fb.code LIKE '%REGISTRATION_PROCESS%'
     AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Kims

FROM user_feedback uf
LEFT JOIN fb_category fb ON uf.category_id = fb.id
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s

UNION ALL


SELECT 'Users Given Feedback' AS Name, COUNT(uf.id) AS AIG,

    (SELECT COUNT(uf.id)
     FROM user_feedback_k uf
     LEFT JOIN fb_category_k fb ON uf.category_id = fb.id
     WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
     AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Kamineni,

    (SELECT COUNT(uf.id)
     FROM user_feedback_a uf
     LEFT JOIN fb_category_a fb ON uf.category_id = fb.id
     WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
     AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Aster,

    (SELECT COUNT(uf.id)
     FROM user_feedback_c uf
     LEFT JOIN fb_category_c fb ON uf.category_id = fb.id
     WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
     AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Continental,

    (SELECT COUNT(uf.id)
     FROM user_feedback_kims uf
     LEFT JOIN fb_category_kims fb ON uf.category_id = fb.id
     WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
     AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s) AS Kims

FROM user_feedback uf
LEFT JOIN fb_category fb ON uf.category_id = fb.id
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
AND DATE(uf.created_at) BETWEEN %(start_date)s AND %(end_date)s;
