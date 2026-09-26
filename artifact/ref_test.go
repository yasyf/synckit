package artifact

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

const (
	digestOne   Digest = "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"
	digestTwo   Digest = "3fc4ccfe745870e2c0d99f71f30ff0656c8dedd41cc1d7d3d376b0dbe685e2f3"
	digestThree Digest = "8b5b9db0c13db24256c829aa364aa90c6d2eba318b9232a4ab9313b954d3555f"
	digestPart  Digest = "37a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed1"
)

func TestSum(t *testing.T) {
	tests := []struct {
		in   string
		want Digest
	}{
		{"one", digestOne},
		{"two", digestTwo},
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
	}
	for _, tt := range tests {
		if got := Sum([]byte(tt.in)); got != tt.want {
			t.Errorf("Sum(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

func TestDigestValidate(t *testing.T) {
	tests := []struct {
		name   string
		digest Digest
		valid  bool
	}{
		{"lowercase hex", digestOne, true},
		{"uppercase", Digest(strings.ToUpper(string(digestOne))), false},
		{"short", digestOne[:63], false},
		{"long", digestOne + "0", false},
		{"non hex", Digest(strings.Repeat("g", 64)), false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.digest.Validate()
			if tt.valid != (err == nil) || (err != nil && !errors.Is(err, ErrInvalid)) {
				t.Fatalf("Validate() = %v, valid %v", err, tt.valid)
			}
		})
	}
}

func TestRefValidate(t *testing.T) {
	tests := []struct {
		name  string
		ref   Ref
		valid bool
	}{
		{"blob", Ref{digestOne, KindBlob, 5}, true},
		{"full blob", Ref{digestOne, KindBlob, ChunkSize}, true},
		{"empty blob", Ref{digestOne, KindBlob, 0}, false},
		{"oversize blob", Ref{digestOne, KindBlob, ChunkSize + 1}, false},
		{"empty group manifest", Ref{digestOne, KindManifest, 0}, true},
		{"large manifest", Ref{digestOne, KindManifest, 10 << 30}, true},
		{"negative manifest", Ref{digestOne, KindManifest, -1}, false},
		{"manifest past float64", Ref{digestOne, KindManifest, 1 << 53}, false},
		{"unknown kind", Ref{digestOne, "tree", 5}, false},
		{"bad digest", Ref{"abc", KindBlob, 5}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ref.Validate()
			if tt.valid != (err == nil) || (err != nil && !errors.Is(err, ErrInvalid)) {
				t.Fatalf("Validate() = %v, valid %v", err, tt.valid)
			}
		})
	}
}

func TestValidateRoots(t *testing.T) {
	tooMany := make([]Ref, MaxRoots+1)
	for i := range tooMany {
		tooMany[i] = Ref{Sum([]byte{byte(i), byte(i >> 8)}), KindBlob, 1}
	}
	tests := []struct {
		name  string
		roots []Ref
		valid bool
	}{
		{"none", nil, true},
		{"distinct", []Ref{{digestOne, KindBlob, 1}, {digestTwo, KindManifest, 9}}, true},
		{"max", tooMany[:MaxRoots], true},
		{"duplicate digest", []Ref{{digestOne, KindBlob, 1}, {digestOne, KindManifest, 1}}, false},
		{"too many", tooMany, false},
		{"invalid root", []Ref{{digestOne, KindBlob, 0}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRoots(tt.roots)
			if tt.valid != (err == nil) || (err != nil && !errors.Is(err, ErrInvalid)) {
				t.Fatalf("ValidateRoots() = %v, valid %v", err, tt.valid)
			}
		})
	}
}

func validManifest() Manifest {
	return Manifest{
		Schema: ManifestSchema,
		Media:  "cc-sync.jsonl",
		Size:   ChunkSize + 5,
		Chunks: []ChunkRef{{digestOne, ChunkSize}, {digestTwo, 5}},
		Deps:   []Ref{{digestThree, KindBlob, 7}},
	}
}

func TestManifestValidate(t *testing.T) {
	tooManyDeps := make([]Ref, MaxDeps+1)
	for i := range tooManyDeps {
		tooManyDeps[i] = Ref{Sum([]byte{byte(i), byte(i >> 8)}), KindBlob, 1}
	}
	tests := []struct {
		name   string
		mutate func(*Manifest)
		valid  bool
	}{
		{"valid", func(*Manifest) {}, true},
		{"pure group", func(m *Manifest) { m.Chunks, m.Size = nil, 0 }, true},
		{"repeated chunk content", func(m *Manifest) { m.Chunks[1].Digest = digestOne }, true},
		{"max deps", func(m *Manifest) { m.Deps = tooManyDeps[:MaxDeps] }, true},
		{"wrong schema", func(m *Manifest) { m.Schema = "synckit.artifact.manifest.v2" }, false},
		{"empty media", func(m *Manifest) { m.Media = "" }, false},
		{"uppercase media", func(m *Manifest) { m.Media = "CC-Sync" }, false},
		{"long media", func(m *Manifest) { m.Media = strings.Repeat("a", 129) }, false},
		{"short non-final chunk", func(m *Manifest) { m.Chunks[0].Size = ChunkSize - 1; m.Size-- }, false},
		{"oversize chunk", func(m *Manifest) { m.Chunks[1].Size = ChunkSize + 1; m.Size = 2*ChunkSize + 1 }, false},
		{"empty chunk", func(m *Manifest) { m.Chunks[1].Size = 0; m.Size = ChunkSize }, false},
		{"size mismatch", func(m *Manifest) { m.Size++ }, false},
		{"bad chunk digest", func(m *Manifest) { m.Chunks[0].Digest = "x" }, false},
		{"duplicate deps", func(m *Manifest) { m.Deps = append(m.Deps, Ref{digestThree, KindManifest, 0}) }, false},
		{"too many deps", func(m *Manifest) { m.Deps = tooManyDeps }, false},
		{"invalid dep", func(m *Manifest) { m.Deps[0].Size = 0 }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := validManifest()
			tt.mutate(&m)
			err := m.Validate()
			if tt.valid != (err == nil) || (err != nil && !errors.Is(err, ErrInvalid)) {
				t.Fatalf("Validate() = %v, valid %v", err, tt.valid)
			}
		})
	}
}

func TestManifestCanonicalEncoding(t *testing.T) {
	want := `{"schema":"synckit.artifact.manifest.v1","media":"cc-sync.jsonl","size":1048581,` +
		`"chunks":[{"digest":"` + string(digestOne) + `","size":1048576},{"digest":"` + string(digestTwo) + `","size":5}],` +
		`"deps":[{"digest":"` + string(digestThree) + `","kind":"blob","size":7}]}`
	encoded, err := validManifest().Encode()
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != want {
		t.Fatalf("Encode() = %s, want %s", encoded, want)
	}
	if got := Sum(encoded); got != "975c52cf630601f0f7679a6dd9bdfe2abea61801eae73505a0ae425b02bc0602" {
		t.Fatalf("manifest digest = %s", got)
	}
	decoded, err := DecodeManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	again, err := decoded.Encode()
	if err != nil || string(again) != want {
		t.Fatalf("round trip = %s, %v", again, err)
	}

	group, err := Manifest{Schema: ManifestSchema, Media: "reposync.tree", Deps: []Ref{{digestOne, KindBlob, 1}}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	wantGroup := `{"schema":"synckit.artifact.manifest.v1","media":"reposync.tree","size":0,"chunks":[],"deps":[{"digest":"` + string(digestOne) + `","kind":"blob","size":1}]}`
	if string(group) != wantGroup {
		t.Fatalf("group Encode() = %s, want %s", group, wantGroup)
	}
}

func TestDecodeManifestRejects(t *testing.T) {
	canonical, err := validManifest().Encode()
	if err != nil {
		t.Fatal(err)
	}
	text := string(canonical)
	tests := []struct {
		name string
		in   string
	}{
		{"unknown field", strings.Replace(text, `"schema"`, `"extra":1,"schema"`, 1)},
		{"reordered fields", strings.Replace(text, `"schema":"synckit.artifact.manifest.v1","media":"cc-sync.jsonl"`, `"media":"cc-sync.jsonl","schema":"synckit.artifact.manifest.v1"`, 1)},
		{"whitespace", strings.Replace(text, `,"media"`, `, "media"`, 1)},
		{"trailing data", text + `{}`},
		{"trailing newline", text + "\n"},
		{"null chunks", `{"schema":"synckit.artifact.manifest.v1","media":"x","size":0,"chunks":null}`},
		{"empty deps array", `{"schema":"synckit.artifact.manifest.v1","media":"x","size":0,"chunks":[],"deps":[]}`},
		{"invalid content", strings.Replace(text, `"size":1048581`, `"size":1048580`, 1)},
		{"not json", "manifest"},
		{"oversize", `{"schema":"` + strings.Repeat("a", MaxManifestBytes) + `"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeManifest([]byte(tt.in)); !errors.Is(err, ErrInvalid) {
				t.Fatalf("DecodeManifest() = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestManifestEncodeBound(t *testing.T) {
	chunks := make([]ChunkRef, 20000)
	for i := range chunks {
		chunks[i] = ChunkRef{digestOne, ChunkSize}
	}
	m := Manifest{Schema: ManifestSchema, Media: "big", Size: int64(len(chunks)) * ChunkSize, Chunks: chunks}
	if _, err := m.Encode(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Encode() of %d chunks = %v, want ErrInvalid", len(chunks), err)
	}
}

func TestObjectEntryValidate(t *testing.T) {
	tests := []struct {
		name  string
		entry ObjectEntry
		valid bool
	}{
		{"blob", ObjectEntry{digestOne, KindBlob, ChunkSize}, true},
		{"manifest", ObjectEntry{digestOne, KindManifest, MaxManifestBytes}, true},
		{"empty blob", ObjectEntry{digestOne, KindBlob, 0}, false},
		{"oversize blob", ObjectEntry{digestOne, KindBlob, ChunkSize + 1}, false},
		{"empty manifest", ObjectEntry{digestOne, KindManifest, 0}, false},
		{"oversize manifest", ObjectEntry{digestOne, KindManifest, MaxManifestBytes + 1}, false},
		{"unknown kind", ObjectEntry{digestOne, "pack", 1}, false},
		{"bad digest", ObjectEntry{"", KindBlob, 1}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.entry.Validate()
			if tt.valid != (err == nil) || (err != nil && !errors.Is(err, ErrInvalid)) {
				t.Fatalf("Validate() = %v, valid %v", err, tt.valid)
			}
		})
	}
}

func testObjects() []ObjectEntry {
	return []ObjectEntry{{digestOne, KindBlob, 5}, {digestTwo, KindManifest, 100}}
}

func TestNewBatchDescriptor(t *testing.T) {
	d, err := NewBatchDescriptor(178, testObjects(), []PartRef{{digestPart, 50}})
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != "274c4b6ca19d23e14fbcf4be94c147707442825deaef9bb5245e3ad1fc6389f7" {
		t.Fatalf("ID = %s", d.ID)
	}
	if d.Schema != BatchSchema || d.Codec != BatchCodec {
		t.Fatalf("schema %q codec %q", d.Schema, d.Codec)
	}
	again, err := NewBatchDescriptor(178, testObjects(), []PartRef{{digestPart, 50}})
	if err != nil || again.ID != d.ID {
		t.Fatalf("second build ID = %s, %v; want %s", again.ID, err, d.ID)
	}
	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"` + string(d.ID) + `","schema":"synckit.artifact.batch.v1","codec":"zstd","raw_size":178,` +
		`"objects":[{"digest":"` + string(digestOne) + `","kind":"blob","size":5},{"digest":"` + string(digestTwo) + `","kind":"manifest","size":100}],` +
		`"parts":[{"digest":"` + string(digestPart) + `","size":50}]}`
	if string(encoded) != want {
		t.Fatalf("descriptor JSON = %s, want %s", encoded, want)
	}
}

func TestPackSize(t *testing.T) {
	tests := []struct {
		name    string
		objects []ObjectEntry
		want    int64
	}{
		{"two small", testObjects(), 4 + (1 + 32 + 1 + 5) + (1 + 32 + 1 + 100) + 1},
		{"two-byte varint", []ObjectEntry{{digestOne, KindBlob, 128}}, 4 + (1 + 32 + 2 + 128) + 1},
		{"full chunk", []ObjectEntry{{digestOne, KindBlob, ChunkSize}}, 4 + (1 + 32 + 3 + ChunkSize) + 1},
	}
	for _, tt := range tests {
		if got := packSize(tt.objects); got != tt.want {
			t.Errorf("%s: packSize() = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestBatchDescriptorValidate(t *testing.T) {
	manyObjects := make([]ObjectEntry, MaxBatchObjects+1)
	for i := range manyObjects {
		manyObjects[i] = ObjectEntry{Sum([]byte{byte(i), byte(i >> 8)}), KindBlob, 1}
	}
	rawObjects := make([]ObjectEntry, 33)
	for i := range rawObjects {
		rawObjects[i] = ObjectEntry{Sum([]byte{byte(i)}), KindBlob, ChunkSize}
	}
	fullParts := func(n int, last int64) []PartRef {
		parts := make([]PartRef, n)
		for i := range parts {
			parts[i] = PartRef{digestPart, PartSize}
		}
		parts[n-1].Size = last
		return parts
	}
	tests := []struct {
		name    string
		rawSize int64
		objects []ObjectEntry
		parts   []PartRef
		mutate  func(*BatchDescriptor)
		valid   bool
	}{
		{name: "valid", rawSize: 178, objects: testObjects(), parts: fullParts(1, 50), valid: true},
		{name: "multi part", rawSize: 178, objects: testObjects(), parts: fullParts(3, 7), valid: true},
		{name: "max objects", rawSize: packSize(manyObjects[:MaxBatchObjects]), objects: manyObjects[:MaxBatchObjects], parts: fullParts(1, 9), valid: true},
		{name: "max raw", rawSize: packSize(rawObjects[:32]), objects: rawObjects[:32], parts: fullParts(33, 9), valid: true},
		{name: "raw size mismatch", rawSize: 177, objects: testObjects(), parts: fullParts(1, 50)},
		{name: "no objects", rawSize: 5, parts: fullParts(1, 50)},
		{name: "too many objects", rawSize: packSize(manyObjects), objects: manyObjects, parts: fullParts(1, 9)},
		{name: "over max raw", rawSize: packSize(rawObjects), objects: rawObjects, parts: fullParts(34, 9)},
		{name: "duplicate object", rawSize: packSize([]ObjectEntry{{digestOne, KindBlob, 5}, {digestOne, KindBlob, 5}}), objects: []ObjectEntry{{digestOne, KindBlob, 5}, {digestOne, KindBlob, 5}}, parts: fullParts(1, 50)},
		{name: "invalid object", rawSize: packSize([]ObjectEntry{{digestOne, KindBlob, 0}}), objects: []ObjectEntry{{digestOne, KindBlob, 0}}, parts: fullParts(1, 50)},
		{name: "no parts", rawSize: 178, objects: testObjects()},
		{name: "too many parts", rawSize: 178, objects: testObjects(), parts: fullParts(maxBatchParts+1, 1)},
		{name: "short non-final part", rawSize: 178, objects: testObjects(), parts: []PartRef{{digestPart, 10}, {digestPart, 10}}},
		{name: "oversize part", rawSize: 178, objects: testObjects(), parts: fullParts(1, PartSize+1)},
		{name: "empty part", rawSize: 178, objects: testObjects(), parts: fullParts(1, 0)},
		{name: "bad part digest", rawSize: 178, objects: testObjects(), parts: []PartRef{{"", 50}}},
		{name: "tampered id", rawSize: 178, objects: testObjects(), parts: fullParts(1, 50), mutate: func(d *BatchDescriptor) { d.ID = digestOne }},
		{name: "tampered object after id", rawSize: 178, objects: testObjects(), parts: fullParts(1, 50), mutate: func(d *BatchDescriptor) { d.Parts[0].Digest = digestOne }},
		{name: "wrong schema", rawSize: 178, objects: testObjects(), parts: fullParts(1, 50), mutate: func(d *BatchDescriptor) { d.Schema = "synckit.artifact.batch.v2" }},
		{name: "wrong codec", rawSize: 178, objects: testObjects(), parts: fullParts(1, 50), mutate: func(d *BatchDescriptor) { d.Codec = "gzip" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := BatchDescriptor{Schema: BatchSchema, Codec: BatchCodec, RawSize: tt.rawSize, Objects: tt.objects, Parts: tt.parts}
			id, err := d.computeID()
			if err != nil {
				t.Fatal(err)
			}
			d.ID = id
			if tt.mutate != nil {
				tt.mutate(&d)
			}
			err = d.Validate()
			if tt.valid != (err == nil) || (err != nil && !errors.Is(err, ErrInvalid)) {
				t.Fatalf("Validate() = %v, valid %v", err, tt.valid)
			}
		})
	}
}

func TestErrorTypes(t *testing.T) {
	var closure *ClosureError
	if err := error(&ClosureError{Bound: BoundDepth, Limit: 32}); !errors.As(err, &closure) || err.Error() != "artifact: closure exceeds the depth bound 32" {
		t.Fatalf("ClosureError = %v", err)
	}
	var missing *MissingError
	if err := error(&MissingError{Digest: digestOne}); !errors.As(err, &missing) || err.Error() != "artifact: object "+string(digestOne)+" is missing" {
		t.Fatalf("MissingError = %v", err)
	}
}

func TestReportJSON(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"commit", CommitReport{Stored: 1, Present: 2, Bytes: 3}, `{"stored":1,"present":2,"bytes":3}`},
		{"gc", GCReport{Marked: 1, Removed: 2, FreedBytes: 3, StagingRemoved: 4}, `{"marked":1,"removed":2,"freed_bytes":3,"staging_removed":4}`},
		{"pins", PinSet{Owner: "o", Roots: []Ref{{digestOne, KindBlob, 1}}}, `{"owner":"o","roots":[{"digest":"` + string(digestOne) + `","kind":"blob","size":1}],"updated_at":"0001-01-01T00:00:00Z"}`},
	}
	for _, tt := range tests {
		encoded, err := json.Marshal(tt.value)
		if err != nil || string(encoded) != tt.want {
			t.Errorf("%s: json = %s, %v; want %s", tt.name, encoded, err, tt.want)
		}
	}
}
