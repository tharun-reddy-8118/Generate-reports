params = tuple([start_date, end_date] * (num_placeholders // 2))
        # conn = get_db_connection()
        # df = pd.read_sql_query(query, conn, params=params)
        # conn.close()

        # df.columns = ['Metric', 'AIG']
        # table_html = df.to_html(index=False, border=1, classes="table", justify='center')