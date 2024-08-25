package server

import (
	"reflect"

	"github.com/gin-gonic/gin"
	"github.com/xeipuuv/gojsonschema"
)

func GenerateJSONSchema(t reflect.Type) map[string]interface{} {
	schema := map[string]interface{}{
		"type":       "object",
		"properties": map[string]interface{}{},
		"required":   []string{},
	}

	properties := schema["properties"].(map[string]interface{})
	required := schema["required"].([]string)

	for i := 0; i < t.Elem().NumField(); i++ {
		field := t.Elem().Field(i)
		fieldName := field.Tag.Get("json")
		if fieldName == "" {
			fieldName = field.Name
		}

		fieldSchema := map[string]interface{}{"type": jsonType(field.Type)}

		// If the field has a "validate" tag with "required", add it to the required list.
		if tag := field.Tag.Get("validate"); tag == "required" {
			required = append(required, fieldName)
		}

		properties[fieldName] = fieldSchema
	}

	return schema
}

func jsonType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Bool:
		return "boolean"
	case reflect.Struct:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	default:
		return "string"
	}
}

func ValidateJSONSchema(schema map[string]interface{}) gin.HandlerFunc {
	return func(c *gin.Context) {
		var payload map[string]interface{}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			c.Abort()
			return
		}

		schemaLoader := gojsonschema.NewGoLoader(schema)
		payloadLoader := gojsonschema.NewGoLoader(payload)

		result, err := gojsonschema.Validate(schemaLoader, payloadLoader)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			c.Abort()
			return
		}

		if !result.Valid() {
			errors := make([]string, 0, len(result.Errors()))
			for _, err := range result.Errors() {
				errors = append(errors, err.String())
			}
			c.JSON(400, gin.H{"errors": errors})
			c.Abort()
			return
		}

		c.Set("payload", payload)
		c.Next()
	}
}
