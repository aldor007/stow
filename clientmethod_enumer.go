// Code generated by "enumer --type=ClientMethod --trimprefix=ClientMethod -json"; DO NOT EDIT.

//
package stow

import (
	"encoding/json"
	"fmt"
)

const _ClientMethodName = "GetPut"

var _ClientMethodIndex = [...]uint8{0, 3, 6}

func (i ClientMethod) String() string {
	if i < 0 || i >= ClientMethod(len(_ClientMethodIndex)-1) {
		return fmt.Sprintf("ClientMethod(%d)", i)
	}
	return _ClientMethodName[_ClientMethodIndex[i]:_ClientMethodIndex[i+1]]
}

var _ClientMethodValues = []ClientMethod{0, 1}

var _ClientMethodNameToValueMap = map[string]ClientMethod{
	_ClientMethodName[0:3]: 0,
	_ClientMethodName[3:6]: 1,
}

// ClientMethodString retrieves an enum value from the enum constants string name.
// Throws an error if the param is not part of the enum.
func ClientMethodString(s string) (ClientMethod, error) {
	if val, ok := _ClientMethodNameToValueMap[s]; ok {
		return val, nil
	}
	return 0, fmt.Errorf("%s does not belong to ClientMethod values", s)
}

// ClientMethodValues returns all values of the enum
func ClientMethodValues() []ClientMethod {
	return _ClientMethodValues
}

// IsAClientMethod returns "true" if the value is listed in the enum definition. "false" otherwise
func (i ClientMethod) IsAClientMethod() bool {
	for _, v := range _ClientMethodValues {
		if i == v {
			return true
		}
	}
	return false
}

// MarshalJSON implements the json.Marshaler interface for ClientMethod
func (i ClientMethod) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for ClientMethod
func (i *ClientMethod) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("ClientMethod should be a string, got %s", data)
	}

	var err error
	*i, err = ClientMethodString(s)
	return err
}
