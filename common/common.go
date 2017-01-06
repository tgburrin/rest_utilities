package common

import (
    "io"
    "errors"
    "net/http"
    "encoding/json"
    "reflect"
)

func SetKey (dict map[string]interface{}, key string, value interface{}) (rv bool, err error) {
    rv = true

    if dict == nil {
        rv = false
        err = errors.New("Uninitialized map passed")
        return rv, err
    }

    _, ok := dict[key]
    if !ok {
        dict[key] = make([]interface{}, 0)
    }
    dict[key] = value

    return rv, err
}

func InterfaceToStringArray ( input interface{} ) ( output []string ) {
    for _, v := range input.([]interface{}) {
        output = append(output, v.(string))
    }
    return output
}

func formatResponse (input map[string]interface{}) (rv map[string]interface{}) {
    rv = make(map[string]interface{})

    val, ok := input["msg"]
    if ok && reflect.TypeOf(val).Elem().Kind() == reflect.String {
        SetKey(rv, "errormsg", val)
    }

    val, ok = input["data"]
    if ok {
        SetKey(rv, "data", val)
    }

    val, ok = input["num"]
    if ok && reflect.TypeOf(val).Kind() == reflect.Int {
        SetKey(rv, "num", val)
    }

    val, ok = input["next"]
    if ok && reflect.TypeOf(val).Kind() == reflect.Map {
        SetKey(rv, "next", val)
    }

    return rv
}

func formatJsonResponse ( input map[string]interface{} ) ( rv string, err error ) {
    brv, err := json.Marshal(formatResponse(input))
    rv = string(brv)
    return rv, err
}

func MakeCreatedReponse(w http.ResponseWriter, location string) {
    w.Header().Set("Location", location)
    w.WriteHeader(http.StatusCreated)
}

func MakeNoContent(w http.ResponseWriter) {
    w.WriteHeader(http.StatusNoContent)
}

func MakeDataResponse (w http.ResponseWriter, input map[string]interface{}) (err error) {
    jsonMsg, err := formatJsonResponse(input)
    if err != nil {
        return err
    }

    io.WriteString(w, jsonMsg+"\n")
    return err
}

func MakeNotFoundResponse (w http.ResponseWriter, input map[string]interface{}) (err error) {
    jsonMsg, err := formatJsonResponse(input)
    if err != nil {
        return err
    }

    w.WriteHeader(http.StatusNotFound)
    io.WriteString(w, jsonMsg+"\n")
    return err
}

func MakeInvalidInputResponse (w http.ResponseWriter, input map[string]interface{}) (err error) {
    jsonMsg, err := formatJsonResponse(input)
    if err != nil {
        return err
    }

    http.Error(w, jsonMsg, 400)
    return err
}

func MakeInvalidMethodResponse (w http.ResponseWriter, input map[string]interface{}) (err error) {
    jsonMsg, err := formatJsonResponse(input)
    if err != nil {
        return err
    }

    http.Error(w, jsonMsg, 405)
    return err
}

func MakeInternalErrorResponse (w http.ResponseWriter, input map[string]interface{}) (err error) {
    jsonMsg, err := formatJsonResponse(input)
    if err != nil {
        return err
    }

    http.Error(w, jsonMsg, 500)
    return err
}

func MakeNotImplementedResponse (w http.ResponseWriter, input map[string]interface{}) (err error) {
    jsonMsg, err := formatJsonResponse(input)
    if err != nil {
        return err
    }

    http.Error(w, jsonMsg, 501)
    return err
}

func MakeUnavailableResponse (w http.ResponseWriter, input map[string]interface{}) (err error) {
    jsonMsg, err := formatJsonResponse(input)
    if err != nil {
        return err
    }

    http.Error(w, jsonMsg, 503)
    return err
}
