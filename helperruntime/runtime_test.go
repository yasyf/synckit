package helperruntime

import (
	"reflect"
	"testing"
)

func TestConfigSurfaceIsExact(t *testing.T) {
	typeOf := reflect.TypeFor[Config]()
	want := []string{"App", "Socket", "Server", "Workers", "Children", "StopStore", "Prepare"}
	if typeOf.NumField() != len(want) {
		t.Fatalf("Config fields = %d, want %d", typeOf.NumField(), len(want))
	}
	for index, name := range want {
		if got := typeOf.Field(index).Name; got != name {
			t.Fatalf("Config field %d = %q, want %q", index, got, name)
		}
	}
}

func TestNewRejectsIncompleteConfiguration(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New accepted an empty config")
	}
}
