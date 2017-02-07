package dal_postgresql

import (
    "testing"
    "os"
    "fmt"

    "database/sql"
    _ "github.com/lib/pq"

    "github.com/satori/go.uuid"
)

func getConnectionSettings () ( con_settings map[string]interface{} ) {
    con_settings = make(map[string]interface{})
    con_settings["ssl"] = false
    return con_settings
}

func getTableSettings () ( tbl_settings map[string]interface{} ) {
    tbl_settings = make(map[string]interface{})
    tbl_settings["table_name"] = "dal_test_1"
    tbl_settings["pk"] = []string{"id"}
    return tbl_settings
}

func TestPostgresDataHandlerInsert (t *testing.T) {
    conn, err := GetDatabaseHandle(getConnectionSettings())
    if err != nil {
        fmt.Println(err.Error())
        t.Fail()
    }

    tbl_settings := getTableSettings()
    dbh, err := NewPostgresDataHandler(conn, tbl_settings)
    if err != nil {
        fmt.Println(err.Error())
        t.Fail()
    }

    rec := make(map[string]interface{})
    rec["id"] = uuid.NewV1().String()
    rec["msg"] = "hello world"

    err = dbh.InsertRecord(rec)
    if err != nil {
        fmt.Println(err.Error())
        t.Fail()
    }

    // Find the inserted record
    db, err := sql.Open("postgres", "sslmode=disable")
    if err != nil {
        fmt.Println(err.Error())
        t.Fail()
    }
    defer db.Close()

    var res_id string
    var res_msg sql.NullString

    fmt.Println("Finding "+rec["id"].(string))
    err = db.QueryRow("select id, msg from "+tbl_settings["table_name"].(string)+
                      " where id='"+rec["id"].(string)+"'").Scan(&res_id, &res_msg)
    if err != nil {
        fmt.Println(err.Error())
        t.Fail()
    }

    if !res_msg.Valid || res_msg.String != rec["msg"].(string) {
        fmt.Println("msg doesn't match between records: '"+res_msg.String+"' vs '"+rec["msg"].(string)+"'")
        t.Fail()
    }
}

func TestMain(m *testing.M) {
    db, err := sql.Open("postgres", "sslmode=disable")
    defer db.Close()

    _, err = db.Exec(`create table IF NOT EXISTS dal_test_1 (
    id uuid not null default uuid_generate_v1(),
    create_dt timestamp,
    json_field jsonb,
    counter bigint,
    msg text,
    primary key (id))`)

    if err != nil {
        fmt.Println("Error: "+err.Error())
        os.Exit(1)
    }

    os.Exit(m.Run())
}
