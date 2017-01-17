package common

import (
    "testing"
)

func TestSetKey (t *testing.T) {
    a := make(map[string]interface{})
    SetKey(a, "this", 10)

    if v, ok := a["this"]; !ok {
        t.Error("Index 'this' does not exist")
    } else if v != 10 {
        t.Error("Value of 'this' is not 10")
    }
}

func TestInterfaceToStringArray (t *testing.T) {
}
