package utils

import "testing"

type TestStruct struct {
	Testing bool `json:"testing"`
}

func TestSetValueByTag(t *testing.T) {
	var s TestStruct
	err := SetValueByTag(&s, "testing", "true", "json")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(s)
}
