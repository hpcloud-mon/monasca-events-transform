import _mysql


def retrieve_transforms():
    base_query = ("select id, specification from event_transform "
                      "where deleted_at IS NULL")

    conn = _mysql.connect(host="localhost", user="monapi",
                              passwd="password", db="mon")
    
    conn.query(base_query)
    r = conn.store_result()
    # maxrows = 0 returns all rows at once
    rows = r.fetch_row(maxrows=0)

    return rows

