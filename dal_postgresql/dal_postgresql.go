package dal_postgresql

import (
    "fmt"
    "errors"
    "bytes"
    "strings"
    "strconv"
    "reflect"
    "database/sql"
    _ "github.com/lib/pq"
)

func GetDatabaseHandle (loginDetails map[string]interface{}) (dbh *sql.DB, err error) {
    var connDetailString string

    connDetail := []string{}
    if hn, ok := loginDetails["hostname"]; ok && strings.HasPrefix(hn.(string), "postgresql://") {
        connDetailString = hn.(string)
    } else {
       for k, v := range loginDetails {
            if k == "type" {
                continue
            }

            switch k {
                case "hostname":
                    connDetail = append(connDetail, "host="+v.(string))
                case "username":
                    connDetail = append(connDetail, "user="+v.(string))
                case "database":
                    connDetail = append(connDetail, "dbname="+v.(string))
                case "port":
                    connDetail = append(connDetail, k+"="+strconv.FormatInt(int64(v.(float64)), 10))
                case "ssl":
                    var mode string
                    if reflect.TypeOf(v).Kind() == reflect.Bool {
                        if v.(bool) == true {
                            mode = "require"
                        } else {
                            mode = "disable"
                        }
                    } else if reflect.TypeOf(v).Kind() == reflect.String {
                        // Not implemented yet
                        switch v.(string) {
                            case "strict":
                            default:
                                mode = "verify-full" // default unknown string, go with strict
                        }
                    }

                    if mode != "" {
                        connDetail = append(connDetail, "sslmode="+mode)
                    }
                default:
                    connDetail = append(connDetail, k+"="+v.(string))
            }
        }
        connDetailString = strings.Join(connDetail, " ")
    }

    dbh, err = sql.Open("postgres", connDetailString)
    if err != nil {
        return dbh, err
    }

    dbh.SetMaxIdleConns(50)
    dbh.SetMaxOpenConns(200)

    return dbh, err
}

type postgresDataHandler struct {
    Id                      map[string]interface{}
    NumAffectedLastOp       int
    RecordLastOp            map[string]interface{}
    Record                  map[string]interface{}

    dbh                     *sql.DB
    tableName               string
    projection              map[string]interface{}
    searchCriteria          map[string]interface{}
    primaryKey              []string
    dmlType                 string
    dmlStatement            string
    dmlArguments            []interface{}
}

func NewPostgresDataHandler (dbh *sql.DB,
                             tableDetails map[string]interface{}) (pDH postgresDataHandler, err error) {
    if dbh == nil {
        return pDH, errors.New("Invalid database handle provided")
    }

    if tableDetails == nil {
        return pDH, errors.New("Invalid tableDetails provided")
    }

    if _, ok := tableDetails["tableName"]; !ok {
        return pDH, errors.New("tableName must be provided")
    }

    if _, ok := tableDetails["pk"]; !ok {
        return pDH, errors.New("Primary Key 'pk' details must be provided")
    }

    pDH = postgresDataHandler{}
    pDH.dbh = dbh
    pDH.NumAffectedLastOp = -1
    pDH.tableName = tableDetails["tableName"].(string)
    pDH.projection = make(map[string]interface{})
    pDH.searchCriteria = make(map[string]interface{})

    for _, f := range tableDetails["pk"].([]string) {
        pDH.projection[f] = nil
        pDH.primaryKey = append(pDH.primaryKey, f)
    }

    return pDH, err
}

func (pDH *postgresDataHandler) GetDMLStatement () (dmlstm string, dmlarg []interface{}) {
    return pDH.dmlStatement, pDH.dmlArguments
}

func (pDH *postgresDataHandler) SetProjection (fieldList []string) {
    for _, field := range fieldList {
        pDH.projection[field] = nil
    }
}

func (pDH *postgresDataHandler) SetFindCriteria (findKeys map[string]interface{}) (err error) {
    for _, pkf := range pDH.primaryKey {
        if _, ok := findKeys[pkf]; !ok {
            return errors.New("Primary key field "+pkf+" is missing")
        }
    }

    pDH.searchCriteria = findKeys
    return err
}

func (pDH *postgresDataHandler) FindRecord () (err error) {
    if pDH.dmlType != "" {
        return errors.New("Record is already staged for "+pDH.dmlType)
    }

    proj := []string{}
    for k, _ := range pDH.projection {
        proj = append(proj, k)
    }

    statement := bytes.NewBufferString("select ")
    statement.WriteString(strings.Join(proj,","))
    statement.WriteString(" from "+pDH.tableName+" where ")

    placeholders := []string{}

    pc := int64(1)
    for k, v := range pDH.searchCriteria {
        placeholders = append(placeholders, k+"=$"+strconv.FormatInt(pc,10))
        pDH.dmlArguments = append(pDH.dmlArguments, v)
        pc += 1
    }

    statement.WriteString(strings.Join(placeholders, " and "))
    pDH.dmlStatement = statement.String()

    rows, err := pDH.dbh.Query(pDH.dmlStatement, pDH.dmlArguments...)
    if err != nil {
        return err
    }

    var row map[string]interface{}
    columns, _ := rows.Columns()
    count := len(columns)

    counter := 0
    for rows.Next() {
        values := make([]interface{}, count)
        valuePtrs := make([]interface{}, count)
        for i, _ := range columns {
            valuePtrs[i] = &values[i]
        }
        rows.Scan(valuePtrs...)

        row = make(map[string]interface{})

        for i, col := range columns {
            var v interface{}
            val := values[i]
            b, ok := val.([]byte)
            if (ok) {
                v = string(b)
            } else {
                v = val
            }

            row[col] = v
        }
        counter += 1
    }

    pDH.NumAffectedLastOp = counter
    if counter > 0 {
        pDH.Record = row
        pDH.RecordLastOp = row
    }

    return err
}

func (pDH *postgresDataHandler) UpdateRecord (args map[string]interface{}) (err error) {
    return err
}

func (pDH *postgresDataHandler) DeleteRecord (args map[string]interface{}) (err error) {
    return err
}

func (pDH *postgresDataHandler) InsertRecord (record map[string]interface{}, args... string) (err error) {
    if pDH.dmlType != "" {
        return errors.New("Record is already stageed for "+pDH.dmlType)
    }

    if len(record) == 0 {
        return errors.New("A blank record was passed")
    }

    return_modified := false
    for _, flag := range args {
        if flag == "return_modified" {
            return_modified = true
        }
    }

    statement := bytes.NewBufferString("insert into "+pDH.tableName+" (")
    fields := []string{}
    placeholders := []string{}
    var values []interface{}

    counter := int64(1)
    for k, v := range record {
        fields = append(fields, k)
        values = append(values, v)
        placeholders = append(placeholders, "$"+strconv.FormatInt(counter,10))
        counter += 1
    }
    statement.WriteString(strings.Join(fields,","))
    statement.WriteString(") values (")
    statement.WriteString(strings.Join(placeholders,","))
    statement.WriteString(")")

    pDH.dmlType = "insert"
    pDH.dmlArguments = values
    if return_modified {
        pDH.dmlStatement = fmt.Sprintf("with inserted_rows as (%s returning %s.*) select * from inserted_rows", statement.String(), pDH.tableName)
        //rowData := make(map[string]interface{})
        rows, err := pDH.dbh.Query(pDH.dmlStatement, pDH.dmlArguments...)
        if err != nil {
            return err
        }

        columns, _ := rows.Columns()
        count := len(columns)

        var row map[string]interface{}

        counter := 0
        for rows.Next() {
            values := make([]interface{}, count)
            valuePtrs := make([]interface{}, count)
            for i, _ := range columns {
                valuePtrs[i] = &values[i]
            }
            rows.Scan(valuePtrs...)

            row = make(map[string]interface{})

            for i, col := range columns {
                var v interface{}
                val := values[i]
                b, ok := val.([]byte)
                if (ok) {
                    v = string(b)
                } else {
                    v = val
                }

                row[col] = v
            }
            counter += 1
        }

        pDH.NumAffectedLastOp = counter
        if counter > 0 {
            pDH.Record = row
            pDH.RecordLastOp = row
        }
    } else {
        pDH.dmlStatement = statement.String()
        _, err = pDH.dbh.Exec(pDH.dmlStatement, pDH.dmlArguments...)
    }

    return err
}
