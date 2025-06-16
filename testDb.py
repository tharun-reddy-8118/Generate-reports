from flask import Flask, render_template, request
import psycopg2
import pandas as pd

app = Flask(__name__)

def get_db_connection():
    return  psycopg2.connect(
        host="52.0.130.35",     
        port="4763",
        database="achala_health_prod_db",
        user="aig_user",
        password="Achala@123$"
    )

@app.route("/", methods=["GET", "POST"])
def report():
    table_html = None
    if request.method == "POST":
        start_date = request.form["start_date"]
        end_date = request.form["end_date"]

        query = f"""
        SELECT 'Kiosks Count' AS Name, COUNT(1)::BIGINT AS AIG,

    (SELECT COUNT(1)::BIGINT FROM device_k d WHERE d."status" = 'ACTIVE' AND d.device_name LIKE '%REG%') AS Kamineni,
    (SELECT COUNT(1)::BIGINT FROM device_a d WHERE d."status" = 'ACTIVE' AND d.device_name LIKE '%KIOSK%') AS Aster,
    (SELECT COUNT(1)::BIGINT FROM device_c d WHERE d."status" = 'ACTIVE' AND d.device_name LIKE '%KIOSK%') AS Continental,
    (SELECT COUNT(1)::BIGINT FROM device_kims d WHERE d."status" = 'ACTIVE') AS Kims

FROM device d
WHERE d."status" = 'ACTIVE' AND d.device_name LIKE '%REG%'

UNION ALL

SELECT 'Total Successful Registrations' AS Name, COUNT(u.id)::BIGINT AS AIG,

(SELECT COUNT(u.id)::BIGINT AS value  
FROM "user_k" u 
LEFT JOIN device_k d ON u.device_id = d.device_code 
WHERE registration_type= 'MOBILE_REGISTRATION' 

AND DATE(u.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Kamineni,


(SELECT COUNT(u.id)::BIGINT AS value
FROM "user_a" u
LEFT JOIN device_a d ON u.device_id = d.device_code
WHERE u.registration_type != 'VITAL'
AND DATE(u.created_at) BETWEEN '{start_date}' AND '{end_date}') AS aster,

(SELECT COUNT(u.id)::BIGINT AS value
FROM "user_c" u
LEFT JOIN device_c d ON u.device_id = d.device_code
WHERE u.registration_type != 'VITAL'
  AND u.created_at::date BETWEEN '{start_date}' AND '{end_date}') AS Continental,

(SELECT COUNT(DISTINCT u.patient_id)::BIGINT AS value
FROM "appointment_details_kims" u
LEFT JOIN device_kims d ON u.device_id = d.device_code
WHERE u.status = 'NEW_PATIENT'
AND DATE(u.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Kims
  
FROM "user" u
LEFT JOIN device d ON u.device_id = d.device_code
WHERE registration_type != 'VITAL'
AND DATE(u.created_at) BETWEEN '{start_date}' AND '{end_date}'

UNION ALL

SELECT 'Avg. Transaction time' AS Name,
EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT AS AIG,

(SELECT EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT AS value 
FROM track_screen_k ts
LEFT JOIN device_k d ON ts.device_id = d.device_code 
WHERE d.device_name LIKE '%REG%' 
AND ts."action" = 'REGISTRATION_SUCCESS' 
AND DATE(ts.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Kamineni,

(SELECT EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT AS value
FROM track_screen_a ts
LEFT JOIN device_a d ON ts.device_id = d.device_code
WHERE d.device_name LIKE '%KIOSK%'
 AND ts."action" = 'REGISTRATION_SUCCESS'
 AND DATE(ts.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Aster,
 
 
(SELECT EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT AS value
FROM track_screen_c ts
LEFT JOIN device_c d ON ts.device_id = d.device_code
WHERE d.device_name LIKE '%KIOSK%'
AND ts."action" = 'REGISTRATION_SUCCESS'
AND ts.created_at::date BETWEEN '{start_date}' AND '{end_date}') AS Continental,

(SELECT EXTRACT(EPOCH FROM AVG(ts.updated_at - ts.created_at))::BIGINT AS value
FROM track_screen_kims ts
LEFT JOIN device_kims d ON ts.device_id = d.device_code
WHERE ts."action" = 'REGISTRATION_SUCCESS'
AND DATE(ts.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Kims

FROM track_screen ts
LEFT JOIN device d ON ts.device_id = d.device_code
WHERE d.device_name LIKE '%REG%'
AND ts."action" = 'REGISTRATION_SUCCESS'
AND DATE(ts.created_at)  BETWEEN '{start_date}' AND '{end_date}'

UNION ALL
SELECT 'Avg. Rating' AS Name,
ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1) AS AIG,

(SELECT  ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1) AS value 
FROM user_feedback_k uf
LEFT JOIN fb_category_k fb ON uf.category_id = fb.id 
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%' 
AND DATE(uf.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Kamineni,

(SELECT 
ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1) AS value
FROM user_feedback_a uf
LEFT JOIN fb_category_a fb ON uf.category_id = fb.id
WHERE (fb."code" LIKE '%REGISTRATION_PROCESS%'
OR fb."code" LIKE '%REPORT_PRINT%')
AND DATE(uf.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Aster,

(SELECT 
ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1) AS value
FROM user_feedback_c uf
LEFT JOIN fb_category_c fb ON uf.category_id = fb.id
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
AND uf.created_at::date BETWEEN '{start_date}' AND '{end_date}') AS Continental,

(SELECT 
ROUND(AVG(uf.rating)::NUMERIC, 1)::NUMERIC(4,1) AS value
FROM user_feedback_kims uf
LEFT JOIN fb_category_kims fb ON uf.category_id = fb.id
WHERE fb.code LIKE '%REGISTRATION_PROCESS%'
AND DATE(uf.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Kims


FROM user_feedback uf
LEFT JOIN fb_category fb ON uf.category_id = fb.id
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
AND DATE(uf.created_at) BETWEEN '{start_date}' AND '{end_date}'

UNION ALL 

SELECT 'Users Given Feedback' AS Name,
COUNT(uf.id) AS VALUE,

(SELECT 
COUNT(uf.id) AS value 
FROM user_feedback_k uf 
LEFT JOIN fb_category_k fb ON uf.category_id = fb.id 
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%' 
AND DATE(uf.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Kamineni,

(SELECT 
COUNT(uf.id) AS value
FROM user_feedback_a uf
LEFT JOIN fb_category_a fb ON uf.category_id = fb.id
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
AND DATE(uf.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Aster,

(SELECT 
COUNT(uf.id) AS value
FROM user_feedback_c uf
LEFT JOIN fb_category_c fb ON uf.category_id = fb.id
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
AND uf.created_at::DATE BETWEEN '{start_date}' AND '{end_date}') AS Continental,


(SELECT 
COUNT(uf.id) AS value
FROM user_feedback_kims uf
LEFT JOIN fb_category_kims fb ON uf.category_id = fb.id
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
AND DATE(uf.created_at) BETWEEN '{start_date}' AND '{end_date}') AS Kims


FROM user_feedback uf
LEFT JOIN fb_category fb ON uf.category_id = fb.id
WHERE fb."code" LIKE '%REGISTRATION_PROCESS%'
AND DATE(uf.created_at) BETWEEN '{start_date}' AND '{end_date}';
"""
        

        conn = get_db_connection()
        df = pd.read_sql_query(query, conn)
        df.loc[2, df.columns[1:]] = df.loc[2, df.columns[1:]].apply(convert_time)
        df.loc[3, df.columns[1:]] = df.loc[3, df.columns[1:]].fillna(0)

        int_rows = [0, 1, 4]  
        for row in int_rows:
            df.iloc[row, 1:] = df.iloc[row, 1:].fillna(0).astype(int)
        conn.close()

        df.columns = ['Metric', 'AIG', 'Kamineni', 'Aster', 'Continental', 'Kims']
        table_html = df.to_html(index=False, border=1, justify="center", classes="report-table")

    return render_template("report.html", table=table_html)
def convert_time(val):
    try:
        seconds = int(val) if not pd.isna(val) else 0
        hrs = seconds // 3600
        mins = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hrs:02}:{mins:02}:{secs:02}"
    except:
        return "00:00:00"
if __name__ == "__main__":
    app.run(debug=True)
