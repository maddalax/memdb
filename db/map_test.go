package db

import "testing"

func TestTrackedMap_Remove(t *testing.T) {
	om := NewTrackedMap[int]()
	om.Set("1", 1)
	om.Set("2", 2)
	om.Set("3", 3)
	om.Remove("2")

	res, _ := om.Get("2")

	if res != nil {
		t.Error("Expected key 2 to be removed")
	}

	values := om.Values()
	if len(values) != 2 {
		t.Error("Expected length to be 2")
	}

	keys := om.Keys()
	if len(keys) != 2 {
		t.Error("Expected length to be 2")
	}

	if len(om.GetPendingDelete()) != 1 {
		t.Error("Expected 1 pending delete")
	}

	om.MarkPersisted("1")

	if len(om.GetPendingDelete()) != 1 {
		t.Error("Expected 1 pending delete")
	}

	if om.keyLength != 2 {
		t.Error("Expected 2 keys")
	}

	om.MarkPersisted("2")

	if len(om.GetPendingDelete()) != 0 {
		t.Error("Expected 0 pending delete")
	}
}

func TestTrackedMap_GetPendingPersist(t *testing.T) {
	om := NewTrackedMap[int]()
	om.Set("1", 1)
	om.Set("2", 2)
	om.Set("3", 3)

	if len(om.GetPendingPersist()) != 3 {
		t.Error("Expected 3 pending persist")
	}

	om.MarkPersisted("1")
	om.MarkPersisted("2")

	if len(om.GetPendingPersist()) != 1 {
		t.Error("Expected 1 pending persist")
	}

	om.MarkPersisted("3")

	if len(om.GetPendingPersist()) != 0 {
		t.Error("Expected 0 pending persist")
	}
}

func TestTrackedMap_Get(t *testing.T) {
	om := NewTrackedMap[int]()
	om.Set("1", 1)
	om.Set("2", 2)
	om.Set("3", 3)

	res, _ := om.Get("2")

	if *res != 2 {
		t.Error("Expected 2")
	}
}

func TestTrackedMap_Items(t *testing.T) {
	om := NewTrackedMap[int]()
	om.Set("1", 1)
	om.Set("2", 2)
	om.Set("3", 3)

	items := om.Items()

	if len(items) != 3 {
		t.Error("Expected 3 items")
	}
}

func TestTrackedMap_KeyLengthMatches(t *testing.T) {
	om := NewTrackedMap[int]()
	om.Set("1", 1)
	om.Set("2", 2)
	om.Set("3", 3)

	if len(om.Keys()) != om.Length() {
		t.Error("expected keys length to match length")
	}

	om.Remove("2")

	if len(om.Keys()) != om.Length() {
		t.Error("expected keys length to match length")
	}
}

func TestTrackedMap_KeyNotExist(t *testing.T) {
	om := NewTrackedMap[int]()
	om.Set("1", 1)
	om.Set("2", 2)
	om.Set("3", 3)

	res, exists := om.Get("4")

	if res != nil {
		t.Error("Expected nil")
	}

	if exists {
		t.Error("Expected false")
	}
}

func TestTrackedMap_MarkingDeleteAlsoMarksPending(t *testing.T) {
	om := NewTrackedMap[int]()
	om.Set("1", 1)
	om.Set("2", 2)
	om.Set("3", 3)

	om.MarkPersisted("1")
	om.MarkPersisted("2")
	om.MarkPersisted("3")

	if len(om.GetPendingDelete()) != 0 {
		t.Error("Expected 0 pending persist")
	}

	om.Remove("2")

	if len(om.GetPendingDelete()) != 1 {
		t.Error("Expected 1 pending delete")
	}

	if len(om.GetPendingPersist()) != 1 {
		t.Error("Expected 1 pending persist")
	}
}
