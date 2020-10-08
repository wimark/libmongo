package libmongo

import (
	"testing"
)

func TestNullDb(t *testing.T) {
	db := MongoDb{}
	if db.IsConnected() == true {
		t.Fatalf("IsConnected not working")
	}

	if err := db.Insert("test", []string{"1", "2"}); err == nil {
		t.Fatalf("Insert with empty not working")
	}
}
