package validation

import (
    "testing"
    "fmt"
)

func TestValidateRequiredFields (t *testing.T) {
    a := make(map[string]interface{})
    setKey(a, "hello", "world")

    b := []string{"hello"}
    if err := ValidateRequiredFields(a, b); err != nil {
        fmt.Println("Validation of required fields failed")
        t.Fail()
    }
}
