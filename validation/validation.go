package validation

import (
    "fmt"
    "errors"
    "reflect"
    "strconv"
    "log"
    "strings"

    "github.com/satori/go.uuid"
)

func setKey ( dict map[string]interface{},
              key string, value interface{} ) ( rv bool, err error ) {
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

func updatefieldList ( fieldDef map[string]interface{},
                       require_an_optional *bool,
                       fieldFilter map[string]interface{} ) {
    if v, ok :=  fieldDef["require_an_optional"]; ok {
        *require_an_optional = v.(bool)
    }
    if v, ok := fieldDef["fields"]; ok {
        for _, f := range v.([]interface{}) {
            fieldFilter[f.(string)] = nil
        }
    }
}

func processRequiredFields ( subsectionName string,
                             required_fields map[string]interface{} ) ( err error ) {

    delete(required_fields, "_comment")

    for _, v := range []string{"initial","all"} {
        _, ok := required_fields[v]
        if !ok {
            fmt.Println(required_fields)
            return errors.New("Both 'all' and 'initial' must be defined in "+
                              subsectionName+": missing "+v)
        }
    }

    for action, rf := range required_fields {
        if reflect.TypeOf(rf).Kind() != reflect.Map {
            return errors.New("'"+action+"' is not a hashmap")
        }

        for k, v := range rf.(map[string]interface{}) {
            if k == "fields" {
                if reflect.TypeOf(v).Kind() != reflect.Slice {
                    return errors.New("'fields' in "+subsectionName+" is not an array of values")
                }
                if err = simpleIntrospection(v.([]interface{}), reflect.String); err != nil {
                    return errors.New("'fields' in "+subsectionName+" mus be a string list of fields")
                }
            } else if k == "require_an_optional" && reflect.TypeOf(v).Kind() != reflect.Bool {
                return errors.New("'require_an_optional' must be set to true or false")
            }
        }
    }

    initial_fields := required_fields["initial"].(map[string]interface{})
    delete(required_fields, "initial")

    all_fields := required_fields["all"].(map[string]interface{})
    delete(required_fields, "all")

    for action, config := range required_fields {
        cfg := config.(map[string]interface{})

        require_an_optional := false
        fieldList := []string{}

        finalDefinition := make(map[string]interface{})
        fieldFilter := make(map[string]interface{})

        updatefieldList(initial_fields, &require_an_optional, fieldFilter)
        updatefieldList(cfg, &require_an_optional, fieldFilter)
        updatefieldList(all_fields, &require_an_optional, fieldFilter)

        for f, _ := range fieldFilter {
            fieldList = append(fieldList, f)
        }

        setKey(finalDefinition, "fields", fieldList)
        setKey(finalDefinition, "require_an_optional", require_an_optional)

        required_fields[action] = finalDefinition
    }

    return err
}

func processLegend ( subsectionName string, legend map[string]interface{} ) ( err error ) {
    for fieldName, _typeDefinition := range legend {
        if reflect.TypeOf(_typeDefinition).Kind() != reflect.Map {
            return errors.New("Invalid type definition map for field "+fieldName+
                              " in section "+subsectionName)
        }

        typeDefinition := _typeDefinition.(map[string]interface{})

        fieldType, ok := typeDefinition["type"]
        if !ok {
            return errors.New("Field "+fieldName+" does not have 'type' in its definition")
        }

        switch fieldType.(string) {
            case "string":
                typeDefinition["type"] = reflect.String
            case "int":
                typeDefinition["type"] = reflect.Int64
            case "float":
                typeDefinition["type"] = reflect.Float64
            case "bool":
                typeDefinition["type"] = reflect.Bool
            case "array":
                typeDefinition["type"] = reflect.Slice
                foundField := false
                if _, ok := typeDefinition["sub_type"]; ok {
                    foundField = true
                    if reflect.TypeOf(typeDefinition["sub_type"]).Kind() != reflect.Map {
                        return errors.New("'sub_type' was not specified as a hashmap in field "+
                                          fieldName+" in "+subsectionName)
                    }
                } else if _, ok := typeDefinition["introspect"]; ok {
                    foundField = true
                    if value, ok := typeDefinition["additional_validation"];
                       !ok || reflect.TypeOf(value).Kind() != reflect.String {
                        return errors.New("'introspect' specified with invalid 'additional_validation' "+
                                          "in field "+fieldName+" in "+subsectionName)
                    }
                }

                if !foundField {
                    return errors.New("'sub_type' or 'introspect' must be specified for field "+
                                      fieldName+" in "+subsectionName)
                }
            case "hashmap":
                typeDefinition["type"] = reflect.Map
            default:
                return errors.New("Unhandled validation type "+fieldType.(string)+" in "+fieldName+
                                  " in section "+subsectionName)
        }

        if st, ok := typeDefinition["sub_type"];
           ok && (typeDefinition["type"] == reflect.Slice || typeDefinition["type"] == reflect.Map) {
            err = parseSubSection( fieldName, st.(map[string]interface{}) )
            if err != nil {
                return err
            }
        }
    }

    return err
}

func parseSubSection ( subsectionName string, subsection map[string]interface{} ) ( err error ) {
    for _, v := range []string{"legend","required_fields"} {
        if s, ok := subsection[v]; !ok || reflect.TypeOf(s).Kind() != reflect.Map {
            return errors.New("Legend field "+v+" is required")
        }
    }

    err = processRequiredFields(subsectionName, subsection["required_fields"].(map[string]interface{}))
    if err != nil {
        return err
    }

    return processLegend(subsectionName, subsection["legend"].(map[string]interface{}))
}

func simpleIntrospection ( arr []interface{}, t reflect.Kind ) ( err error ) {
    for _, v := range arr {
        if reflect.TypeOf(v).Kind() != t {
            return errors.New("Invalid value found")
        }
    }
    return err
}

func InitSchema ( schema map[string]interface{} ) ( err error ) {
    return parseSubSection("root", schema)
}

func ValidatieAnOptionalField ( input map[string]interface{},
                                fieldDefs map[string]interface{},
                                requiredFields []string ) ( err error ) {
    hasOptional := false
    for k, _ := range fieldDefs {
        isOptional := true

        for _, rk := range requiredFields {
            if k == rk {
                isOptional = false
            }
        }

        if isOptional {
            for ik, _ := range input {
                if k == ik {
                    hasOptional = true
                }
            }
        }
    }

    if !hasOptional {
        err = errors.New("At least one optional field must be provided")
    }

    return err
}

func ValidateWithSchema ( input map[string]interface{},
                          schema map[string]interface{}, action string ) ( err error ) {
    reqFields := schema["required_fields"].(map[string]interface{})

    var requiredFields []string
    var requireOptional bool

    if rf, ok := reqFields[action]; ok {
        r := rf.(map[string]interface{})
        requiredFields = r["fields"].([]string)
        requireOptional = r["require_an_optional"].(bool)
    } else {
        r := reqFields["default"].(map[string]interface{})
        requiredFields = r["fields"].([]string)
        requireOptional = r["require_an_optional"].(bool)
    }

    err = ValidateRequiredFields(input, requiredFields)
    if err != nil {
        return err
    }

    if requireOptional {
        err = ValidatieAnOptionalField ( input,
                                         schema["legend"].(map[string]interface{}),
                                         requiredFields )
        if err != nil {
            return err
        }
    }

    err = ValidateWithLegend(input, schema["legend"].(map[string]interface{}), action)
    if err != nil {
        return err
    }
    return err
}

func ValidateWithLegend ( input map[string]interface{},
                          legend map[string]interface{}, action string ) ( err error ) {
    for inputKey, inputValue := range input {
        validationMethod, ok := legend[inputKey].(map[string]interface{})
        if !ok {
            // This is a junk value that was passed and should be strained out
            // fmt.Println("Deleting unknown field "+inputKey)
            delete(input, inputKey)
            continue
        }

        // Every validation block must have a 'type' assigned to it
        // Ideally we'd be able to check the specific type before we move onto the next step
        if _, ok := validationMethod["type"]; !ok {
            return errors.New("'type' not defined for key "+inputKey)
        }

        // Any field that is allowed to be 'null' and whoes value is null
        // can be handled without checking type
        nullOk := false
        if val, ok := validationMethod["null_ok"]; ok && reflect.TypeOf(val).Kind() == reflect.Bool {
            nullOk = val.(bool)
        }

        if nullOk && inputValue == nil {
            continue
        } else if inputValue == nil {
            return errors.New(inputKey+" may not be set to null")
        }

        // Time to check the type
        switch validationMethod["type"] {
            case reflect.String:
                if reflect.TypeOf(inputValue).Kind() != reflect.String {
                    return errors.New(inputKey+" is not a valid string")
                }

                addtnlValidation, ok := validationMethod["additional_validation"]
                // Additional string validation to take place
                if ok {
                    if reflect.TypeOf(addtnlValidation).Kind() == reflect.String {
                        switch addtnlValidation {
                            case "UUID":
                                uuidValue, err := uuid.FromString(inputValue.(string))
                                if err != nil {
                                    return err
                                }
                                inputValue = uuidValue
                            default:
                                log.Println("Unknown additional validation type "+
                                            addtnlValidation.(string))
                        }
                    } else if reflect.TypeOf(addtnlValidation).Kind() == reflect.Map {
                        enum_list, ok := (addtnlValidation.(map[string]interface{}))["enum"]
                        if ok {
                            valueFound := false
                            strlist := []string{}
                            for _, v := range enum_list.([]interface{}) {
                                strlist = append(strlist, v.(string))
                                if inputValue.(string) == v.(string) {
                                    valueFound = true
                                }
                            }
                            if !valueFound {
                                return errors.New("Value for '"+inputKey+"' must be in the list: "+
                                                    strings.Join(strlist, ", "))
                            }
                        }

                        // Need to check for [min,max], [equals], [min,"max"], and ["min",max]
                        // The string values of "min" and "max" will indicate unbounded lower and upper
                        size_spec, ok := (addtnlValidation.(map[string]interface{}))["length"].
                                         ([]interface{})
                        if ok {
                            valueLength := float64(len([]rune(inputValue.(string))))
                            // An exact length
                            if len(size_spec) == 1 &&
                               reflect.TypeOf(size_spec[0]).Kind() == reflect.Float64 {
                                if valueLength != size_spec[0] {
                                    return errors.New("'"+inputValue.(string)+"' must be exactly "+
                                                        size_spec[0].(string)+" characters long")
                                }
                            // Upper and lower bounds
                            } else if len(size_spec) == 2 {
                                lower := size_spec[0]
                                upper := size_spec[1]

                                lower_t := reflect.TypeOf(lower).Kind()
                                upper_t := reflect.TypeOf(upper).Kind()

                                // Validation of schema def
                                if lower_t == reflect.Float64 && upper_t == reflect.Float64 &&
                                   lower.(float64) >= upper.(float64) {
                                    return errors.New("Lower bound must be smaller than higher bound")
                                }

                                if lower_t == reflect.String && lower != "min" {
                                    return errors.New("Lower bound must be 'min' if it is a string")
                                }

                                if upper_t == reflect.String && upper != "max" {
                                    return errors.New("Upper bound must be 'max' if it is a string")
                                }

                                // Validation of input
                                if lower_t == reflect.Float64 && valueLength < lower.(float64) {
                                    return errors.New(inputKey+": '"+inputValue.(string)+
                                        "' is smaller than min length of "+
                                        strconv.FormatFloat(lower.(float64), 'f', 0, 64))
                                }

                                if upper_t == reflect.Float64 && valueLength > upper.(float64) {
                                    return errors.New(inputKey+": '"+inputValue.(string)+
                                        "' is larger than max length of "+
                                        strconv.FormatFloat(upper.(float64), 'f', 0, 64))
                                }
                                
                            } else {
                                return errors.New("Invalid 'length' specification for string")
                            }
                        }
                    }
                }
                input[inputKey] = inputValue
            case reflect.Int64:
                if reflect.TypeOf(inputValue).Kind() == reflect.String {
                    inputValue, err = strconv.ParseInt(inputValue.(string), 10, 64)
                    if err != nil {
                        return err
                    }
                }
                if reflect.TypeOf(inputValue).Kind() != reflect.Int64 {
                    return errors.New(inputKey+" is not a valid integer")
                }
                input[inputKey] = inputValue
            case reflect.Bool:
                if reflect.TypeOf(inputValue).Kind() != reflect.Bool {
                    return errors.New(inputKey+" is not a valid boolean")
                }
            case reflect.Map:
                if reflect.TypeOf(inputValue).Kind() != reflect.Map {
                    return errors.New(inputKey+" is not a valid hashmap")
                }
                err = ValidateWithSchema(inputValue.(map[string]interface{}),
                                         validationMethod["sub_type"].(map[string]interface{}),
                                         action)
                if err != nil {
                    return err
                }
            case reflect.Slice:
                if reflect.TypeOf(inputValue).Kind() != reflect.Slice {
                    return errors.New(inputKey+" is not a valid array")
                }
                if sub_key, ok := validationMethod["sub_type"]; ok {
                    err = ValidateWithSchema(inputValue.(map[string]interface{}),
                                             sub_key.(map[string]interface{}),
                                             action)
                    if err != nil {
                        return err
                    }
                } else if _, ok := validationMethod["introspect"]; ok {
                }
            default:
                return errors.New("Unhandled validation type for "+inputKey+" as "+
                                  validationMethod["type"].(reflect.Kind).String())
        }
    }

    return err
}

func ValidateRequiredFields ( input map[string]interface{}, reqfields []string ) ( err error ) {
    for _, reqfield := range reqfields {
        if _, ok := input[reqfield]; !ok {
            return errors.New("Field "+reqfield+" is a required field")
        }
    }
    return err
}
